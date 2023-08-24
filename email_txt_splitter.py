import os
import re


def save_list_as_eml(messages: list, output_folder: str):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for idx, message in enumerate(messages, start=1):
        eml_file_path = os.path.join(output_folder, f"message_{idx}.eml")

        with open(eml_file_path, "w") as eml_file:
            eml_file.write(message)



regex_pattern = r"^From\s+\S+\s+(at)\s"
kept_messages = []
this_body = ''
lines = text_collection.splitlines()

for i, line in enumerate(lines):
    match = re.match(regex_pattern, line)
    if not match:
        this_body += line + '\n'
    else:
        kept_messages.append(this_body)
        this_body = ''
kept_messages.append(this_body)

save_list_as_eml(kept_messages, 'archive emls')