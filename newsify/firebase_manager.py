import firebase_admin
from firebase_admin import credentials, firestore
from scrapy.utils.project import get_project_settings

class FirebaseManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseManager, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance

    def initialize(self):
        settings = get_project_settings()
        cred_path = settings.get('FIREBASE_CRED_PATH')
        if not cred_path:
            raise ValueError("FIREBASE_CRED_PATH is not set in Scrapy settings")
        
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    @property
    def client(self):
        return self.db

# Don't create an instance here. Let it be created when needed.