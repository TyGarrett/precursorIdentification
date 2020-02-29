public class ms1Line {

    public double mass;
    public double intensity;
    public double ionMobility;
    public int charge = 2;

    public ms1Line(double m, double i, double im){
        this.mass = m;
        this.intensity = i;
        this.ionMobility = im;
    }
    public double getMass(){
        return mass;
    }
    public double getIntensity(){
        return intensity;
    }

    public ms1Line(String line){
        String[] arr = line.split(" ");
        this.mass = Double.parseDouble(arr[0]);
        this.intensity = Double.parseDouble(arr[1]);
        this.ionMobility = Double.parseDouble(arr[2]);
    }

    public int compareTo(ms1Line line) {
        return (this.intensity > line.intensity ? -1 :
                (this.intensity == line.intensity ? 0 : 1));
    }

    public String toString(){
        return "Mass: " + mass + ", Intensity: " + intensity + ", ionMobility: " + ionMobility;
    }
}


