import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
from datetime import datetime
from server.constants import REGION_CONFIGS, REGION_BOUNDS

class Coordinator:
    def __init__(self):
        self.connections = {}
        self._initialize_connections()
    
    def _initialize_connections(self):
        for region, config in REGION_CONFIGS.items():
            try:
                conn = psycopg2.connect(
                    host=config['host'],
                    port=config['port'],
                    dbname=config['database'],
                    user=config['user'],
                    password=config['password']
                )
                self.connections[region] = conn
            except Exception as e:
                print(f"Warning: Could not connect to {region}: {e}")
    
    def _determine_region(self, latitude, longitude):
        for region, bounds in REGION_BOUNDS.items():
            if bounds['lat_min'] <= latitude <= bounds['lat_max'] and bounds['lon_min'] <= longitude <= bounds['lon_max']:
                return region
        return 'us-east'
    
    def _get_connection(self, region):
        if region not in self.connections:
            raise ValueError(f"Region {region} not configured")
        return self.connections[region]
    
    def create_ride(self, user_id, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon,
                    driver_id=None, status='requested', price=0.0, distance_km=0.0,
                    duration_minutes=0, pickup_geohash=None):
        region = self._determine_region(pickup_lat, pickup_lon)
        conn = self._get_connection(region)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        ride_id = str(uuid.uuid4())
        
        try:
            cursor.execute("""
                INSERT INTO rides (ride_id, user_id, driver_id, pickup_lat, pickup_lon,
                                 dropoff_lat, dropoff_lon, region, pickup_geohash,
                                 status, price, distance_km, duration_minutes, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING *
            """, (ride_id, user_id, driver_id, pickup_lat, pickup_lon,
                  dropoff_lat, dropoff_lon, region, pickup_geohash,
                  status, price, distance_km, duration_minutes))
            
            result = cursor.fetchone()
            conn.commit()
            return dict(result) if result else {}
        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to create ride: {e}")
        finally:
            cursor.close()
    
    def get_ride(self, ride_id):
        for region in REGION_CONFIGS.keys():
            conn = self._get_connection(region)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            try:
                cursor.execute("""
                    SELECT * FROM rides WHERE ride_id = %s
                """, (ride_id,))
                
                result = cursor.fetchone()
                if result:
                    cursor.close()
                    return dict(result)
            except Exception as e:
                pass
            finally:
                cursor.close()
        
        return None
    
    def get_rides(self, region=None, user_id=None, driver_id=None, status=None, limit=100):
        all_rides = []
        
        if region and region not in REGION_CONFIGS:
            return []
        
        regions_to_search = [region] if region else REGION_CONFIGS.keys()
        
        for reg in regions_to_search:
            conn = self._get_connection(reg)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            try:
                query = "SELECT * FROM rides WHERE 1=1"
                params = []
                
                if region:
                    query += " AND region = %s"
                    params.append(region)
                
                if user_id:
                    query += " AND user_id = %s"
                    params.append(user_id)
                
                if driver_id:
                    query += " AND driver_id = %s"
                    params.append(driver_id)
                
                if status:
                    query += " AND status = %s"
                    params.append(status)
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                for row in results:
                    all_rides.append(dict(row))
            except Exception as e:
                pass
            finally:
                cursor.close()
        
        all_rides.sort(key=lambda x: x.get('timestamp', datetime.min), reverse=True)
        limit = int(limit) if limit else 100
        return all_rides[:limit]
    
    def update_ride(self, ride_id, **kwargs):
        ride = self.get_ride(ride_id)
        if not ride:
            return None
        
        original_region = ride['region']
        pickup_lat = kwargs.get('pickup_lat', ride['pickup_lat'])
        pickup_lon = kwargs.get('pickup_lon', ride['pickup_lon'])
        new_region = self._determine_region(pickup_lat, pickup_lon)
        
        if new_region != original_region:
            old_conn = self._get_connection(original_region)
            new_conn = self._get_connection(new_region)
            old_cursor = old_conn.cursor(cursor_factory=RealDictCursor)
            
            try:
                old_cursor.execute("SELECT * FROM rides WHERE ride_id = %s", (ride_id,))
                ride_data = old_cursor.fetchone()
                if not ride_data:
                    old_cursor.close()
                    return None
                
                ride_dict = dict(ride_data)
                ride_dict.update(kwargs)
                ride_dict['region'] = new_region
                
                old_cursor.execute("DELETE FROM rides WHERE ride_id = %s", (ride_id,))
                old_conn.commit()
                old_cursor.close()
                
                new_cursor = new_conn.cursor(cursor_factory=RealDictCursor)
                new_cursor.execute("""
                    INSERT INTO rides (ride_id, user_id, driver_id, pickup_lat, pickup_lon,
                                     dropoff_lat, dropoff_lon, region, pickup_geohash,
                                     status, price, distance_km, duration_minutes, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                """, (ride_dict['ride_id'], ride_dict['user_id'], ride_dict.get('driver_id'),
                      ride_dict['pickup_lat'], ride_dict['pickup_lon'],
                      ride_dict['dropoff_lat'], ride_dict['dropoff_lon'],
                      ride_dict['region'], ride_dict.get('pickup_geohash'),
                      ride_dict['status'], ride_dict['price'],
                      ride_dict['distance_km'], ride_dict['duration_minutes'],
                      ride_dict['timestamp']))
                
                result = new_cursor.fetchone()
                new_conn.commit()
                new_cursor.close()
                return dict(result) if result else ride_dict
            except Exception as e:
                old_conn.rollback()
                new_conn.rollback()
                raise Exception(f"Failed to update ride: {e}")
        
        conn = self._get_connection(original_region)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        allowed_fields = ['driver_id', 'status', 'price', 'distance_km', 'duration_minutes',
                         'pickup_lat', 'pickup_lon', 'dropoff_lat', 'dropoff_lon', 'pickup_geohash']
        
        update_fields = []
        params = []
        for field, value in kwargs.items():
            if field in allowed_fields and value is not None:
                update_fields.append(f"{field} = %s")
                params.append(value)
        
        if 'pickup_lat' in kwargs or 'pickup_lon' in kwargs:
            update_fields.append("region = %s")
            params.append(new_region)
        
        if not update_fields:
            cursor.close()
            return ride
        
        params.append(ride_id)
        query = f"UPDATE rides SET {', '.join(update_fields)} WHERE ride_id = %s RETURNING *"
        
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            conn.commit()
            return dict(result) if result else ride
        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to update ride: {e}")
        finally:
            cursor.close()
    
    def delete_ride(self, ride_id):
        ride = self.get_ride(ride_id)
        if not ride:
            return False
        
        region = ride['region']
        conn = self._get_connection(region)
        cursor = conn.cursor()
        
        try:
            cursor.execute("DELETE FROM rides WHERE ride_id = %s", (ride_id,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to delete ride: {e}")
        finally:
            cursor.close()
    
    def close_all_connections(self):
        for region, conn in self.connections.items():
            try:
                conn.close()
            except Exception as e:
                print(f"Error closing connection to {region}: {e}")

coordinator = Coordinator()

