from db import db

class mission(db.Model):
    __tablename__ = 'mission'
    id = db.Column(db.Integer, primary_key=True)
    mission_type = db.Column(db.String(50))
    takeoff_location = db.Column(db.String(50))
    time_over_targettime_over_target = db.Column(db.Float)
    bomb_damage_assessment = db.Column(db.String(50))
    country = db.Column(db.String(50))
    mission_date = db.Column(db.Date, nullable=False)
    target_name = db.Column(db.String(50))
    target_type = db.Column(db.String(50))
    target_country = db.Column(db.String(50))
    target_city = db.Column(db.String)
    mission_name = db.Column(db.String(50), nullable=False)

    def to_dict(self):
        return {
            'mission_type': self.mission_type,
            'takeoff_location': self.takeoff_location,
            'time_over_targettime_over_target': self.time_over_targettime_over_target,
            'bomb_damage_assessment': self.bomb_damage_assessment,
            'country': self.country,
            'mission_date': self.mission_date,
            'target_name': self.target_name,
            'target_type': self.target_type,
            'target_country': self.target_country,
            'target_city': self.target_city,
            'mission_name': self.mission_name
        }

    def __repr__(self):
        return f'<Mission {self.mission_name}>'