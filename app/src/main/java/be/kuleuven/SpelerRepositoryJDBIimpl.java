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
    String resultString = null;

    resultString = jdbi.withHandle(handle -> {
      return handle.createQuery(
          "SELECT t.clubnaam, w.finale, w.winnaar " +
          "FROM wedstrijd w " +
          "JOIN tornooi t ON w.tornooi = t.id " +
          "WHERE (w.speler1 = :spelerId OR w.speler2 = :spelerId) " +
          "ORDER BY CASE WHEN w.finale = 1 AND w.winnaar = :spelerId THEN 0 ELSE w.finale END ASC;")
        .bind("spelerId", speler.getTennisvlaanderenId())
        .map((rs, ctx) -> {
          String clubnaam = rs.getString("clubnaam");
          int finale = rs.getInt("finale");
          int winnaar = rs.getInt("winnaar");
          String finaleString;

          if (finale == 1 && winnaar == tennisvlaanderenid) {
            finaleString = "winst";
          } else if (finale == 1) {
            finaleString = "finale";
          } else if (finale == 2) {
            finaleString = "halve finale";
          } else if (finale == 4) {
            finaleString = "kwart finale";
          } else {
            finaleString = "lager dan kwart finale";
          }

          return "Hoogst geplaatst in het tornooi van " + clubnaam + " met plaats in de " + finaleString;
        })
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Geen resultaten gevonden voor speler met ID: " + tennisvlaanderenid));
    });

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
