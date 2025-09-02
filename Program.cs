using System;
using System.IO;
using System.Messaging;
using System.Text;

namespace ImportQueue
{
    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                if (args.Length != 2)
                {
                    Console.WriteLine("Usage: ImportQueue.exe <csvFileName> <queueName>");
                    Console.WriteLine("Example: ImportQueue.exe \"ccms_to_hub_biztalk.csv\" \"CJISBIZDEV\\Private$\\ccms_to_hub_biztalk\"");
                    PressAnyKey();
                    return;
                }

                string csvPath = args[0];
                string queueName = args[1];

                Console.WriteLine($"Reading from CSV: {Path.GetFullPath(csvPath)}");
                Console.WriteLine($"Sending to queue: {queueName}");

                // Import messages from CSV to queue
                ImportMessages(csvPath, queueName);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            }

            PressAnyKey();
        }

        private static void ImportMessages(string csvPath, string queueName)
        {
            try
            {
                // Validate CSV file exists
                if (!File.Exists(csvPath))
                {
                    Console.WriteLine($"ERROR: CSV file does not exist: {csvPath}");
                    return;
                }

                // Validate/create destination queue
                string cleanQueueName = queueName;
                if (queueName.EndsWith(";journal", StringComparison.OrdinalIgnoreCase))
                {
                    cleanQueueName = queueName.Substring(0, queueName.Length - 8);
                }

                if (!MessageQueue.Exists(cleanQueueName))
                {
                    Console.WriteLine($"Creating destination queue: {cleanQueueName}");
                    MessageQueue.Create(cleanQueueName);
                }

                using (MessageQueue messageQueue = new MessageQueue(queueName))
                {
                    // Use XML formatter like CJISBIZPROD instead of ActiveX
                    messageQueue.Formatter = new XmlMessageFormatter(new Type[]
                    {
                        typeof(string),
                        typeof(byte[]),
                        typeof(object),
                        typeof(int),
                        typeof(DateTime)
                    });

                    Console.WriteLine("Reading CSV file...");
                    string[] lines = File.ReadAllLines(csvPath, Encoding.UTF8);

                    if (lines.Length <= 1)
                    {
                        Console.WriteLine("ERROR: CSV file is empty or contains only header");
                        return;
                    }

                    // Skip header line
                    int totalMessages = lines.Length - 1;
                    Console.WriteLine($"Found {totalMessages} messages to import");

                    int imported = 0;
                    int errors = 0;

                    for (int i = 1; i < lines.Length; i++) // Start from 1 to skip header                    
                    {
                        try
                        {
                            string line = lines[i];
                            if (string.IsNullOrWhiteSpace(line))
                                continue;

                            Message message = ParseCSVLineToMessage(line);
                            if (message != null)
                            {
                                messageQueue.Send(message);
                                imported++;

                                if (imported % 100 == 0)
                                {
                                    Console.Write($"\rImported: {imported}/{totalMessages}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            errors++;
                            Console.WriteLine($"\nError importing line {i}: {ex.Message}");
                        }
                    }

                    Console.WriteLine($"\n✓ Successfully imported {imported}/{totalMessages} messages");
                    if (errors > 0)
                    {
                        Console.WriteLine($"⚠ {errors} messages had errors and were skipped");
                    }
                }
            }
            catch (UnauthorizedAccessException)
            {
                Console.WriteLine("ERROR: Access denied. Run as administrator or check queue permissions.");
            }
            catch (MessageQueueException ex)
            {
                Console.WriteLine($"ERROR: Message Queue error: {ex.Message}");
                Console.WriteLine($"Error Code: {ex.MessageQueueErrorCode}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error importing messages: {ex.Message}");
                throw;
            }
        }

        private static Message ParseCSVLineToMessage(string csvLine)
        {
            try
            {
                // Simple CSV parser - splits by comma but handles quoted values
                string[] fields = ParseCSVLine(csvLine);

                // Expected CSV format from ExportQueue:
                // QueueName,MessageId,CorrelationId,Label,Body,Priority,
                // Recoverable,AppSpecific,SentTime,ArrivedTime,
                // TimeToReachQueue,TimeToBeReceived,UseJournalQueue,UseDeadLetterQueue,
                // ImportedAt,OriginalBodyType,MessageSize

                if (fields.Length < 5) // At minimum we need QueueName, MessageId, CorrelationId, Label, Body
                {
                    Console.WriteLine($"Skipping line - insufficient fields: {fields.Length}");
                    return null;
                }

                Message message = new Message();

                // Body (index 4) - most important field
                string body = fields[4];
                if (!string.IsNullOrEmpty(body))
                {
                    // Send as binary stream to avoid MSMQ string serialization
                    byte[] bodyBytes = Encoding.UTF8.GetBytes(body);
                    message.BodyStream = new System.IO.MemoryStream(bodyBytes);
                    message.BodyType = 0; // VT_EMPTY - let MSMQ handle it as raw data
                }
                else
                {
                    message.Body = body;
                }

                // Label (index 3)
                if (fields.Length > 3 && !string.IsNullOrEmpty(fields[3]))
                {
                    message.Label = fields[3];
                }
                else
                {
                    message.Label = "Imported from CSV";
                }

                // Priority (index 5)
                if (fields.Length > 5 && !string.IsNullOrEmpty(fields[5]))
                {
                    if (int.TryParse(fields[5], out int priority))
                    {
                        message.Priority = (MessagePriority)Math.Min(Math.Max(priority, 0), 7);
                    }
                }

                // Recoverable (index 6)
                if (fields.Length > 6 && !string.IsNullOrEmpty(fields[6]))
                {
                    message.Recoverable = fields[6] == "1" || fields[6].ToLower() == "true";
                }

                // AppSpecific (index 7)
                if (fields.Length > 7 && !string.IsNullOrEmpty(fields[7]))
                {
                    if (int.TryParse(fields[7], out int appSpecific))
                    {
                        message.AppSpecific = appSpecific;
                    }
                }

                // UseJournalQueue (index 12)
                if (fields.Length > 12 && !string.IsNullOrEmpty(fields[12]))
                {
                    message.UseJournalQueue = fields[12] == "1" || fields[12].ToLower() == "true";
                }

                // UseDeadLetterQueue (index 13)
                if (fields.Length > 13 && !string.IsNullOrEmpty(fields[13]))
                {
                    message.UseDeadLetterQueue = fields[13] == "1" || fields[13].ToLower() == "true";
                }

                return message;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing CSV line: {ex.Message}");
                return null;
            }
        }

        private static string[] ParseCSVLine(string line)
        {
            // Simple CSV parser that handles quoted fields
            var fields = new System.Collections.Generic.List<string>();
            bool inQuotes = false;
            StringBuilder field = new StringBuilder();

            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];

                if (c == '"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        // Double quote - add single quote to field
                        field.Append('"');
                        i++; // Skip next quote
                    }
                    else
                    {
                        // Toggle quote state
                        inQuotes = !inQuotes;
                    }
                }
                else if (c == ',' && !inQuotes)
                {
                    // Field separator
                    fields.Add(field.ToString());
                    field.Clear();
                }
                else
                {
                    field.Append(c);
                }
            }

            // Add last field
            fields.Add(field.ToString());

            return fields.ToArray();
        }

        private static void PressAnyKey(string prompt = "Press any key to exit...")
        {
            Console.WriteLine(prompt);
            Console.ReadKey();
        }
    }
}