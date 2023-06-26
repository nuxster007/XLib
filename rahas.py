import tkinter as tk
from cryptography.fernet import Fernet
import hashlib
import random
import socket
import string
from datetime import datetime, timedelta
import json

APP_VERSION = "15-Dec-22"

MAX_WORD_SIZE = 99          # Use to check before encrypting
WORD_SIZE_DIGIT = 3         # Number of digits must aligned with MAX_WORD_SIZE
WORD_SPLITER = "<<-->>"
ENCRYPTED_KEY_SIZE = 44     # Fix by Fernet
ENCRYPTED_DATA_SIZE = 7295
HASH_SIZE = 145             # must more than number of digits of hash number generated (about 19-22)
HASH_SIZE_DIGIT = 3         # Number of digits must aligned with HASH_SIZE
TOTAL_POSITION_SIZE = 4     # Size of all positions in concatenated string
POSITION_DIGIT_SIZE = 4     # keep 4 digits string of position of each char in word (position between 0001 to 9999)

def get_version():
    return APP_VERSION

def encrypt(user, pw,):
    #print("Encrypting ... ")
    hostname = socket.gethostname()
    ipaddr = socket.gethostbyname(hostname)
    curtime = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    word = user + WORD_SPLITER + pw + WORD_SPLITER + hostname + WORD_SPLITER + ipaddr + WORD_SPLITER + curtime + WORD_SPLITER+ APP_VERSION
    word_len = len(word)    # length of word to be encrypted

    # We will encrypt the word into this scheme
    """
    <reverted_key   :   44  >   ENCRYPTED_KEY_SIZE
    <hash_len       :   3   >   length of HASH_SIZE --> HASH_SIZE_DIGIT --> just wanna keep 3 digits
    <hash_data      :   x   >   depend hash generating
    <hash_suffix    :   50-x>   HASH_SIZE = hash_data + hash_suffix
    <word_len       :   3   >   len(word) --> WORD_SIZE_DIGIT --> just wanna keep 3 digits
    <position_len   :   4   >   len(position_list) --> should not more than 9999
    <position_list  :   y   >   format = aaaabbbbccccdddd   --> follow POSITION_DIGIT_SIZE
    <data>
    """
    
    # Random the position of each character in word
    # We know the word length but we need to keep the fake position list to make decrypting more harder
    pos_size = round(word_len*random.randint(2,5)*1.23)

    # Calculate position length
    position_len = POSITION_DIGIT_SIZE * pos_size

    # So this is the size of the header
    header_size = ENCRYPTED_KEY_SIZE + HASH_SIZE_DIGIT + HASH_SIZE + WORD_SIZE_DIGIT + POSITION_DIGIT_SIZE + position_len

    # And the size of the data
    data_size = ENCRYPTED_DATA_SIZE - header_size

    # Let random position to keep each character in word
    pos = []
    while len(pos) != pos_size:
        n = random.randint(1, 9999)
        if (n not in pos) and (n < data_size):
            pos += [n]     

    # Here is concatenated string of all char positions
    position_list = ''.join([f"{p:0{POSITION_DIGIT_SIZE}d}" for p in pos])  # fix each position equal to 4 chars to make it able to calculate back
    #print(f"Position List: {position_list}")

    # Let create data
    data = ""
    word_pos = 0
    for i in range(data_size):
        if i in pos[:word_len]:    # if the data position is in the word then keep the char and move next
            p = pos.index(i)
            data += word[p]
            word_pos += 1
        else:       # else -> just keep random char
            data += random.SystemRandom().choice(string.ascii_letters + string.digits + string.punctuation)
            #data += "-"

    # Ensemble all info before hashing
    data = f"{word_len:0{WORD_SIZE_DIGIT}d}{position_len:0{TOTAL_POSITION_SIZE}d}{position_list}{data}"

    # Now calculate hash key of data (prepare for check integrity during decrypting)
    #hash_data = hash(data)
    hash_data = hashlib.sha3_512(data.encode("ascii")).hexdigest()
    hash_len = len(str(hash_data))

    #print(f"Data for Hashing: {data[:50]} ... {data[-50:]}")
    #print(f"DH Length: {len(data)}")

    #print(f"Hash Length: {hash_len}\tHash Data: {hash_data}")
    #print(f"Word Length: {word_len}\tPosition Length: {position_len}")
    #print(f"Position List:\n{position_list}")
    #print(f"Data: {data[:400]}\t{data[-20:]}")
    #print(f"Length Data:  {len(data)}")

    # Add some number suffix here to make hash key to be fix length at hash_size
    hash_suffix = ''.join([random.SystemRandom().choice(string.digits) for i in range(HASH_SIZE - hash_len)])

    # Ensenble all info again before final encryption
    new_data = f"{hash_len:0{HASH_SIZE_DIGIT}d}{hash_data}{hash_suffix}{data}"

    # Generate key & encrypt
    key = Fernet.generate_key()
    f = Fernet(key)
    enc = f.encrypt(new_data.encode('ascii'))
    #print(enc, len(enc))

    # Do swap key to make it more harder to find actual key
    # Key lenght is fix at 44 (change require if length is not 44)
    reverted_key = key[:22][-1::-1] + key[22:][-1::-1]
    #print(key, reverted_key)

    return f"{reverted_key.decode('ascii')}{enc.decode('ascii')}"

def decrypt(encrypted_data):
    try:
        #print("Decrypting ...")
        
        key = encrypted_data[:22][-1::-1] + encrypted_data[22:44][-1::-1]

        f = Fernet(key)
        encoded_data = key + f.decrypt(encrypted_data[44:].encode("ascii")).decode('ascii')

        a = ENCRYPTED_KEY_SIZE
        hash_len = int(encoded_data[a:a+HASH_SIZE_DIGIT])
        a += HASH_SIZE_DIGIT
        hash_data = encoded_data[a:a+hash_len]
        a += HASH_SIZE
        word_len = int(encoded_data[a:a+WORD_SIZE_DIGIT])
        a += WORD_SIZE_DIGIT
        position_len = int(encoded_data[a:a+TOTAL_POSITION_SIZE])
        a += TOTAL_POSITION_SIZE
        position_list = encoded_data[a:a+position_len]
        #print(position_list)
        a += position_len

        """
        <reverted_key   :   44  >   ENCRYPTED_KEY_SIZE
        <hash_len       :   3   >   length of HASH_SIZE --> HASH_SIZE_DIGIT --> just wanna keep 3 digits
        <hash_data      :   x   >   depend hash generating
        <hash_suffix    :   50-x>   HASH_SIZE = hash_data + hash_suffix
        <word_len       :   3   >   len(word) --> WORD_SIZE_DIGIT --> just wanna keep 3 digits
        <position_len   :   4   >   len(position_list) --> should not more than 9999
        <position_list  :   y   >   format = aaaabbbbccccdddd   --> follow POSITION_DIGIT_SIZE
        <data>
        """

        position_start = ENCRYPTED_KEY_SIZE + HASH_SIZE_DIGIT + HASH_SIZE + WORD_SIZE_DIGIT + TOTAL_POSITION_SIZE
        data_start = position_start + position_len
        data = encoded_data[data_start:]

        #print(f"Hash Length: {hash_len}\tHash Data: {hash_data}")
        #print(f"Word Length: {word_len}\tPosition Length: {position_len}")
        #print(f"Position List:\n{position_list}")
        #print(f"Data: {data[:20]}\t{data[-20:]}")
        #print(f"Length Data:  {len(data)}")

        # Double check hash key to ensure there is no modification of the file
        h_data = f"{word_len:0{WORD_SIZE_DIGIT}d}{position_len:0{TOTAL_POSITION_SIZE}d}{position_list}{data}"
        hash_check = hashlib.sha3_512(h_data.encode("ascii")).hexdigest()
        if hash_check != hash_data:
            return ["ERROR", "Hash Key Mismatch"]

        the_word = ""
        for i in range(word_len):
            t = position_list[POSITION_DIGIT_SIZE*i:POSITION_DIGIT_SIZE*(i+1)]
            #print(t)
            pos = int(t)
            the_word += data[pos]

        return the_word.split(WORD_SPLITER)
    except Exception as ex:
        return ["ERROR", ex]

def GenerateAPIKey(Username):
    # UserName|Generate Data|Expire Date|Valid IP Address|Key Version|Generator
    info = Username + "|" + datetime.strftime(datetime.now(), "%Y-%m-%d") + "|" + datetime.strftime(datetime.now() + timedelta(days=180), "%Y-%m-%d") + "|10.7.30.173|Version 1.0|Tapparit"

    # Generate key & encrypt
    key = Fernet.generate_key()
    f = Fernet(key)
    enc = f.encrypt(info.encode('ascii'))
    #print(enc, len(enc))

    # Do swap key to make it more harder to find actual key
    # Key lenght is fix at 44 (change require if length is not 44)
    reverted_key = key[:22][-1::-1] + key[22:][-1::-1]

    return f"{reverted_key.decode('ascii')}{enc.decode('ascii')}"

def ValidateAPIKey(APIKey):
    try:
        key = APIKey[:22][-1::-1] + APIKey[22:44][-1::-1]

        f = Fernet(key)
        data = f.decrypt(APIKey[44:].encode("ascii")).decode('ascii')

        if (datetime.strptime(data, "%Y-%m-%d") - datetime.now()).total_seconds() > 0:
            return True
        else:
            return True
    
    except Exception as ex:
        return False

def CmdSubmit():
    #print("-"*100)

    if userText.get() != "" and (" " not in userText.get()) and passText.get() != "" and (" " not in passText.get()):
        ec = encrypt(userText.get(), passText.get())
        dc = decrypt(ec)
        if dc[0] == userText.get() and dc[1] == passText.get():
            with open("x.enc", "w") as f:
                f.write(ec)

if __name__ == "__main__":

    key = GenerateAPIKey("Tapparit")
    print(key)
    print(len(key))

    # print(ValidateAPIKey(key))

    OpenGUI  = False
    
    if OpenGUI:
        gui = tk.Tk()
        gui.title(f"Encryption ({APP_VERSION})")
        gui.geometry("300x75")
        gui.resizable(width=False, height=False)

        Label1 = tk.Label(text="Key")
        Label1.grid(row=0, column=0, columnspan=2, padx=20 , pady=5)
        userText = tk.Entry()
        userText.grid(row=0, column=2, columnspan=1, padx=0 , pady=5)

        Label2 = tk.Label(text="Pass")
        Label2.grid(row=1, column=0, columnspan=2, padx=20 , pady=5)
        passText = tk.Entry(show="*")
        passText.grid(row=1, column=2, columnspan=1, padx=0 , pady=5)

        buttonSubmit = tk.Button(gui, text="Generate\nEncrypted File",fg="black", bg="#EC7063", state="normal", command=CmdSubmit)
        buttonSubmit.grid(row=0, column=3, rowspan=2, columnspan=2, padx= 10 , pady=0)

        gui.mainloop()