import csv
import json

transcript_file = 'all_transcripts_output3.csv'
mapping_file = 'personid_mapping3.csv'
output_file = 'conversation_for_ai3.csv'
TRANSCRIPT_JSON_COL = 'transcript'  # Change if needed

def try_csv_reader(filename, encodings=('utf-8', 'cp1252')):
    """Try to open a CSV file with multiple encodings, return reader and file object."""
    for enc in encodings:
        try:
            f = open(filename, newline='', encoding=enc)
            reader = csv.DictReader(f)
            # Try reading the header to force decode
            _ = reader.fieldnames
            return reader, f
        except UnicodeDecodeError:
            try:
                f.close()
            except Exception:
                pass
    raise Exception(f"Could not decode {filename} with tried encodings.")

# Load mapping into a dictionary
mapping_reader, mapping_f = try_csv_reader(mapping_file)
mapping = {}
for row in mapping_reader:
    key = (row['call_id'], str(row['personId']))
    mapping[key] = row['name_or_email']
mapping_f.close()

num_calls = 0
num_errors = 0

transcript_reader, transcript_f = try_csv_reader(transcript_file)
with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=['call_id', 'transcripts'])
    writer.writeheader()
    for row in transcript_reader:
        call_id = row['call_id']
        try:
            transcript = json.loads(row[TRANSCRIPT_JSON_COL])
            lines = []
            for utterance in transcript:
                pid = str(utterance.get('personId'))
                speaker = mapping.get((call_id, pid), f"Person {pid}")
                text = utterance.get('text', '')
                lines.append(f"{speaker}: {text}")
            conversation = '\n'.join(lines)
            writer.writerow({'call_id': call_id, 'transcripts': conversation})
            num_calls += 1
        except Exception as e:
            print(f"Error processing call_id {call_id}: {e}")
            num_errors += 1
transcript_f.close()

print(f"Done! {num_calls} calls processed. {num_errors} errors.")
print(f"Conversation CSV saved to {output_file}")
