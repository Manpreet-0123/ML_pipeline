from src.components.data_ingestion import DataIngestion
from src.components.model_trainer import ModelTrainer

class Training:
    def pipeline(self):
        self.ingestion = DataIngestion()
        raw_data_path,train_data_path,test_data_path,time = self.ingestion.dataset_creation()
        self.model_trainer = ModelTrainer(time)
        model_path = self.model_trainer.model_train(train_data_path)

        return raw_data_path,model_path