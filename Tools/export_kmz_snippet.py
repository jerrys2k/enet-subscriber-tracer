from flask import send_file, request
import sqlite3
import simplekml
from geopy.distance import distance
import os

def create_sector(lat, lon, azimuth, beamwidth, radius_km):
    points = [(lon, lat)]
    start = azimuth - beamwidth / 2
    end = azimuth + beamwidth / 2
    step = max(1, int(beamwidth / 30))
    for angle in range(int(start), int(end) + 1, step):
        d = distance(kilometers=radius_km).destination((lat, lon), angle)
        points.append((d.longitude, d.latitude))
    points.append((lon, lat))
    return points

@app.route("/export_kmz")
def export_kmz():
    msisdn = request.args.get("number")
    if not msisdn or not msisdn.isdigit():
        return "Invalid MSISDN", 400

    db_path = os.path.join(os.path.dirname(__file__), "logs", "trace.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT tower_name, lat, lon, enodeb_id, cell_id, timestamp
        FROM latest_traces
        WHERE msisdn = ? OR msisdn = ?
    """, (msisdn, "592" + msisdn))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return f"No trace data found for {msisdn}", 404

    tower, lat, lon, enodeb_id, cell_id, timestamp = row

    kml = simplekml.Kml()
    pnt = kml.newpoint(name=msisdn, coords=[(lon, lat)])
    pnt.description = f"Tower: {tower}\neNodeB: {enodeb_id} | Cell: {cell_id}\nTime: {timestamp}"
    pnt.style.iconstyle.icon.href = "http://maps.google.com/mapfiles/kml/shapes/target.png"
    pnt.style.labelstyle.scale = 1.2

    sector = kml.newpolygon(name=f"{tower} Sector")
    sector.outerboundaryis = create_sector(lat, lon, azimuth=0, beamwidth=120, radius_km=1.0)
    sector.style.polystyle.color = simplekml.Color.changealphaint(100, simplekml.Color.blue)
    sector.style.linestyle.color = simplekml.Color.white

    filename = f"trace_{msisdn}.kmz"
    export_path = os.path.join("/tmp", filename)
    kml.savekmz(export_path)
    return send_file(export_path, as_attachment=True, download_name=filename)
