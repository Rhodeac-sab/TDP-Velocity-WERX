import re
import datetime
import os
import concurrent.futures

def mask_ip(ip):
    """Mask IP address with 'xxx.xxx.xxx.xxx' format."""
    return re.sub(r'\d', 'x', ip)

def mask_mac(mac):
    """Mask MAC address with 'xx:xx:xx:xx:xx:xx' format."""
    return re.sub(r'[0-9a-fA-F]', 'x', mac)

def mask_password(password):
    """Mask password with '****'."""
    return '****'

def mask_email(email):
    """Mask email address."""
    return re.sub(r'(?<=@)[^\.]+(?=\.)', '****', email)

def mask_phone(phone):
    """Mask phone number."""
    return re.sub(r'(\d{3})[-.\s]?(\d{3})[-.\s]?(\d{4})', r'\1-***-****', phone)

def mask_credit_card(card_number):
    """Mask credit card number."""
    return re.sub(r'\d{12}(\d{4})', r'**** **** **** \1', card_number)

def sanitize_line(line):
    """Sanitize a single line of log by masking sensitive information."""
    try:
        ip_regex = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
        mac_regex = r'\b(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}\b'
        password_regex = r'(?<=password=)[^\s]*'
        email_regex = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}'
        phone_regex = r'\+?\d{1,3}[-.\s]?\(?\d{1,4}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}'
        card_regex = r'\b(?:\d{4}[-\s]?){3}\d{4}\b'

        # Masking sensitive information
        line = re.sub(ip_regex, lambda x: mask_ip(x.group()), line)
        line = re.sub(mac_regex, lambda x: mask_mac(x.group()), line)
        line = re.sub(password_regex, lambda x: mask_password(x.group()), line)
        line = re.sub(email_regex, lambda x: mask_email(x.group()), line)
        line = re.sub(phone_regex, lambda x: mask_phone(x.group()), line)
        line = re.sub(card_regex, lambda x: mask_credit_card(x.group()), line)

        return line
    except Exception as e:
        print(f"Error sanitizing line: {e}")
        return line

def sanitize_log(input_file_path, output_file_path):
    """Sanitize a log file by masking sensitive data and writing it to a new file."""
    try:
        # Open the input file and output file for writing
        with open(input_file_path, 'r') as infile, open(output_file_path, 'w') as outfile:
            lines = infile.readlines()

            # Using ThreadPoolExecutor for multithreading
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Process lines in parallel
                sanitized_lines = executor.map(sanitize_line, lines)

                # Write the sanitized lines to the output file
                for sanitized_line in sanitized_lines:
                    outfile.write(sanitized_line)
        
        print(f"Sanitized log file created: {output_file_path}")

    except FileNotFoundError:
        print(f"Error: The file {input_file_path} was not found.")
    except IOError as e:
        print(f"Error reading/writing file: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def get_sanitized_log_filename(input_file_path):
    """Generate a sanitized log file name based on the current date and time."""
    current_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_file_name = f"{os.path.splitext(input_file_path)[0]}_Sanitized_{current_time}.log"
    return output_file_name

# Example usage
input_log = 'logfile.txt'  # Replace with your log file
sanitized_log = get_sanitized_log_filename(input_log)
sanitize_log(input_log, sanitized_log)
