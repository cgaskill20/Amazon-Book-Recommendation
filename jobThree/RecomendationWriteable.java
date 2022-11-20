import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RecomendationWriteable implements Writable {
  private Text recomendationTitle;
	private IntWritable ocurences;
	
	public RecomendationWriteable(Text recomendationTitle, IntWritable ocurences){
		this.recomendationTitle = recomendationTitle;
		this.ocurences = ocurences;
	}

	public RecomendationWriteable() {
		this.recomendationTitle = new Text("");
		this.ocurences = new IntWritable(0);
	}

	public Text getRecomendationTitle() {
		return recomendationTitle;
	}

	public IntWritable getOcurences() {
		return ocurences;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		recomendationTitle.readFields(arg0);
		ocurences.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		recomendationTitle.write(arg0);
		ocurences.write(arg0);
	}

	@Override
	public String toString(){
		return recomendationTitle.toString() + " " + ocurences;
	}

    @Override
    public int hashCode() {
      return ocurences.hashCode();
    }
}
