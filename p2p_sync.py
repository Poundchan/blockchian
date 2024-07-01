import socket  # นำเข้าโมดูลสำหรับการสื่อสารผ่านเครือข่าย
import threading  # นำเข้าโมดูลสำหรับการทำงานแบบหลายเธรด
import json  # นำเข้าโมดูลสำหรับการจัดการข้อมูล JSON
import sys  # นำเข้าโมดูลสำหรับการเข้าถึงพารามิเตอร์ของระบบ
import os  # นำเข้าโมดูลสำหรับการจัดการไฟล์และระบบปฏิบัติการ
import secrets  # นำเข้าโมดูลสำหรับการสร้างค่าสุ่มที่ปลอดภัย

class Node:
    def __init__(self, host, port):
        self.host = host  # กำหนด host address
        self.port = port  # กำหนดพอร์ต
        self.peers = []  # สร้างลิสต์ว่างสำหรับเก็บ peers
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # สร้าง socket สำหรับการสื่อสาร TCP/IP
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # ตั้งค่า socket ให้สามารถใช้ address ซ้ำได้
        self.transactions = []  # สร้างลิสต์ว่างสำหรับเก็บธุรกรรม
        self.transaction_file = f"transactions_{port}.json"  # กำหนดชื่อไฟล์สำหรับบันทึกธุรกรรม
        self.wallet_address = self.generate_wallet_address()  # สร้าง wallet address

    def generate_wallet_address(self):
        return '0x' + secrets.token_hex(20)  # สร้าง wallet address แบบสุ่มโดยใช้ secrets.token_hex

    def start(self):
        self.socket.bind((self.host, self.port))  # ผูก socket กับ host และ port
        self.socket.listen(1)  # เริ่มรับการเชื่อมต่อ โดยกำหนดคิวการเชื่อมต่อสูงสุด 1
        print(f"Node listening on {self.host}:{self.port}")  # แสดงข้อความว่าโหนดกำลังรับการเชื่อมต่อ
        print(f"Your wallet address is: {self.wallet_address}")  # แสดง wallet address ของโหนด

        self.load_transactions()  # โหลดธุรกรรมจากไฟล์ (ถ้ามี)

        accept_thread = threading.Thread(target=self.accept_connections)  # สร้างเธรดสำหรับรับการเชื่อมต่อ
        accept_thread.start()  # เริ่มการทำงานของเธรด

    def accept_connections(self):
        while True:
            client_socket, address = self.socket.accept()  # รับการเชื่อมต่อใหม่
            print(f"New connection from {address}")  # แสดงข้อความเมื่อมีการเชื่อมต่อใหม่

            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))  # สร้างเธรดใหม่สำหรับจัดการการเชื่อมต่อ
            client_thread.start()  # เริ่มการทำงานของเธรด

    def handle_client(self, client_socket):
        while True:
            try:
                data = client_socket.recv(1024)  # รับข้อมูลจากไคลเอนต์ (ขนาดสูงสุด 1024 ไบต์)
                if not data:
                    break
                message = json.loads(data.decode('utf-8'))  # แปลงข้อมูลจาก bytes เป็น JSON
                
                self.process_message(message, client_socket)  # ประมวลผลข้อความที่ได้รับ

            except Exception as e:
                print(f"Error handling client: {e}")  # แสดงข้อความเมื่อเกิดข้อผิดพลาด
                break

        client_socket.close()  # ปิดการเชื่อมต่อเมื่อจบการทำงาน

    def connect_to_peer(self, peer_host, peer_port):
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # สร้าง socket ใหม่
            peer_socket.connect((peer_host, peer_port))  # เชื่อมต่อไปยัง peer
            self.peers.append(peer_socket)  # เพิ่ม socket ของ peer ลงในลิสต์
            print(f"Connected to peer {peer_host}:{peer_port}")  # แสดงข้อความเมื่อเชื่อมต่อสำเร็จ

            self.request_sync(peer_socket)  # ขอซิงค์ข้อมูลจาก peer

            peer_thread = threading.Thread(target=self.handle_client, args=(peer_socket,))  # สร้างเธรดสำหรับจัดการการสื่อสารกับ peer
            peer_thread.start()  # เริ่มการทำงานของเธรด

        except Exception as e:
            print(f"Error connecting to peer: {e}")  # แสดงข้อความเมื่อเกิดข้อผิดพลาดในการเชื่อมต่อ

    def broadcast(self, message):
        for peer_socket in self.peers:  # วนลูปผ่านทุก peer ที่เชื่อมต่ออยู่
            try:
                peer_socket.send(json.dumps(message).encode('utf-8'))  # ส่งข้อความไปยัง peer
            except Exception as e:
                print(f"Error broadcasting to peer: {e}")  # แสดงข้อความเมื่อเกิดข้อผิดพลาดในการส่งข้อมูล
                self.peers.remove(peer_socket)  # ลบ peer ที่มีปัญหาออกจากลิสต์

    def process_message(self, message, client_socket):
        if message['type'] == 'transaction':  # ถ้าเป็นข้อความประเภทธุรกรรม
            print(f"Received transaction: {message['data']}")  # แสดงข้อมูลธุรกรรมที่ได้รับ
            self.add_transaction(message['data'])  # เพิ่มธุรกรรม
        elif message['type'] == 'sync_request':  # ถ้าเป็นคำขอซิงค์ข้อมูล
            self.send_all_transactions(client_socket)  # ส่งธุรกรรมทั้งหมดไปยังผู้ขอ
        elif message['type'] == 'sync_response':  # ถ้าเป็นการตอบกลับการซิงค์
            self.receive_sync_data(message['data'])  # รับข้อมูลซิงค์
        else:
            print(f"Received message: {message}")  # แสดงข้อความที่ได้รับ (กรณีอื่นๆ)

    def add_transaction(self, transaction):
        if transaction not in self.transactions:  # ถ้าธุรกรรมยังไม่มีในลิสต์
            self.transactions.append(transaction)  # เพิ่มธุรกรรมลงในลิสต์
            self.save_transactions()  # บันทึกธุรกรรมลงไฟล์
            print(f"Transaction added and saved: {transaction}")  # แสดงข้อความยืนยันการเพิ่มและบันทึกธุรกรรม

    def create_transaction(self, recipient, amount):
        transaction = {
            'sender': self.wallet_address,
            'recipient': recipient,
            'amount': amount
        }  # สร้างธุรกรรมใหม่
        self.add_transaction(transaction)  # เพิ่มธุรกรรม
        self.broadcast({'type': 'transaction', 'data': transaction})  # กระจายธุรกรรมไปยัง peers

    def save_transactions(self):
        with open(self.transaction_file, 'w') as f:
            json.dump(self.transactions, f)  # บันทึกธุรกรรมทั้งหมดลงไฟล์ JSON

    def load_transactions(self):
        if os.path.exists(self.transaction_file):  # ถ้าไฟล์ธุรกรรมมีอยู่
            with open(self.transaction_file, 'r') as f:
                self.transactions = json.load(f)  # โหลดธุรกรรมจากไฟล์
            print(f"Loaded {len(self.transactions)} transactions from file.")  # แสดงจำนวนธุรกรรมที่โหลด

    def request_sync(self, peer_socket):
        sync_request = json.dumps({"type": "sync_request"}).encode('utf-8')  # สร้างคำขอซิงค์
        peer_socket.send(sync_request)  # ส่งคำขอซิงค์ไปยัง peer

    def send_all_transactions(self, client_socket):
        sync_data = json.dumps({
            "type": "sync_response",
            "data": self.transactions
        }).encode('utf-8')  # สร้างข้อมูลซิงค์
        client_socket.send(sync_data)  # ส่งข้อมูลซิงค์ไปยังผู้ขอ

    def receive_sync_data(self, sync_transactions):
        for tx in sync_transactions:  # วนลูปผ่านทุกธุรกรรมที่ได้รับ
            self.add_transaction(tx)  # เพิ่มธุรกรรม
        print(f"Synchronized {len(sync_transactions)} transactions.")  # แสดงจำนวนธุรกรรมที่ซิงค์

if __name__ == "__main__":
    if len(sys.argv) != 2:  # ถ้าไม่ได้ระบุพอร์ต
        print("Usage: python script.py <port>")  # แสดงวิธีใช้งานที่ถูกต้อง
        sys.exit(1)  # ออกจากโปรแกรมด้วยรหัส 1 (มีข้อผิดพลาด)
    
    port = int(sys.argv[1])  # แปลงพอร์ตเป็นตัวเลข
    node = Node("0.0.0.0", port)  # สร้างโหนดใหม่
    node.start()  # เริ่มการทำงานของโหนด
    
    while True:
        print("\n1. Connect to a peer")
        print("2. Create a transaction")
        print("3. View all transactions")
        print("4. View my wallet address")
        print("5. Exit")
        choice = input("Enter your choice: ")  # รับค่าตัวเลือกจากผู้ใช้
        
        if choice == '1':
            peer_host = input("Enter peer host to connect: ")  # รับ host ของ peer
            peer_port = int(input("Enter peer port to connect: "))  # รับพอร์ตของ peer
            node.connect_to_peer(peer_host, peer_port)  # เชื่อมต่อไปยัง peer
        elif choice == '2':
            recipient = input("Enter recipient wallet address: ")  # รับที่อยู่กระเป๋าเงินของผู้รับ
            amount = float(input("Enter amount: "))  # รับจำนวนเงิน
            node.create_transaction(recipient, amount)  # สร้างธุรกรรมใหม่
        elif choice == '3':
            print("All transactions:")
            for tx in node.transactions:
                print(tx)  # แสดงธุรกรรมทั้งหมด
        elif choice == '4':
            print(f"Your wallet address is: {node.wallet_address}")  # แสดงที่อยู่กระเป๋าเงินของโหนด
        elif choice == '5':
            break  # ออกจากลูป (จบการทำงาน)
        else:
            print("Invalid choice. Please try again.")  # แสดงข้อความเมื่อเลือกตัวเลือกไม่ถูกต้อง

    print("Exiting...")  # แสดงข้อความเมื่อออกจากโปรแกรม
