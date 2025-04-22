package be.kuleuven;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.checkerframework.checker.units.qual.t;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultBearing;
import org.jdbi.v3.core.result.ResultIterable;

public class SpelerRepositoryJDBIimpl implements SpelerRepository {
  private final Jdbi jdbi;

  // Constructor
  SpelerRepositoryJDBIimpl(String connectionString, String user, String pwd) {
    jdbi = Jdbi.create(connectionString, user, pwd);
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    jdbi.withHandle(handle -> {
      return handle.execute("INSERT INTO speler (tennisvlaanderenid, naam, punten) VALUES (?, ?, ?)",
          speler.getTennisvlaanderenId(), speler.getNaam(), speler.getPunten());
    });
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler speler = (Speler) jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenid = :nummer")
          .bind("nummer", tennisvlaanderenId)
          .mapToBean(Speler.class)
          .findOne()
          .orElseThrow(() -> new InvalidSpelerException("Invalid Speler met identification: " + tennisvlaanderenId + ""));
    });
    return speler;
  }

  @Override
  public List<Speler> getAllSpelers() {
    return jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT * FROM speler")
          .mapToBean(Speler.class)
          .list();
    });
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle
          .createUpdate(
              "UPDATE speler SET naam = :naam, punten = :punten WHERE tennisvlaanderenid = :tennisvlaanderenId ;")
          .bindBean(speler)
          .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException("Invalid Speler met identification: " + speler.getTennisvlaanderenId());
    }
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle
          .createUpdate(
              "DELETE FROM speler WHERE tennisvlaanderenid = :nummer;")
          .bind("nummer", tennisvlaanderenid)
          .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException("Invalid Speler met identification: " + tennisvlaanderenid);
    }
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    Speler speler = getSpelerByTennisvlaanderenId(tennisvlaanderenid); // Check if speler exists
    List<Wedstrijd> resultList = jdbi.withHandle(handle -> {
      return (handle.createQuery(
        "SELECT * " +
        "FROM wedstrijd " +
        "WHERE (speler1 = :spelerId OR speler2 = :spelerId) " +
        "ORDER BY finale DESC;")
        .bind("spelerId", speler.getTennisvlaanderenId()))
        .mapToBean(Wedstrijd.class)
        .list();
      });
    String clubnaam = null;
    String finaleString = null;
    Wedstrijd wedstrijd = null;
      while(resultList.iterator().hasNext()) {
        wedstrijd = resultList.iterator().next();
        if (wedstrijd.getFinale() == 1) { 
          if (wedstrijd.getWinnaarId() == speler.getTennisvlaanderenId()) {
            finaleString = "winst";
            break;
          } else {
            finaleString = "finale";
          }  
        }else if(wedstrijd.getFinale() == 2){
          finaleString = "halve finale";
        }else if(wedstrijd.getFinale() == 4){
          finaleString = "kwart finale";
        }else{
          finaleString = "lager dan kwart finale";
        }
      }  
    // tornooi id ophalen
    int tornooiId = wedstrijd.getTornooiId();
    // clubnaam ophalen
    clubnaam = jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT clubnaam FROM tornooi WHERE id = :id")
          .bind("id", tornooiId)
          .mapTo(String.class)
          .findOne()
          .orElse(null);
    });    
      
    String resultString = "Hoogst geplaatst in het tornooi van " + clubnaam + " met plaats in de " + finaleString;
    return resultString;
  }


  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'addSpelerToTornooi'");
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'removeSpelerFromTornooi'");
  }
}
