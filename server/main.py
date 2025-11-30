from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Ride-sharing api running..."}

@app.get("/rides")
def get_rides():
    return {"message": "Rides fetched successfully"}

@app.get("/rides/{ride_id}")
def get_ride(ride_id):
    return {"message": "Ride fetched successfully"}

@app.post("/rides")
def create_ride(ride):
    return {"message": "Ride created successfully"}

@app.put("/rides/{ride_id}")
def update_ride(ride_id, ride):
    return {"message": "Ride updated successfully"}

@app.delete("/rides/{ride_id}")
def delete_ride(ride_id):
    return {"message": "Ride deleted successfully"}
