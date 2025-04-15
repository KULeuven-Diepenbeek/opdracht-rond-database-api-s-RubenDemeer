package be.kuleuven;

import java.util.List;

import org.jdbi.v3.core.Jdbi;

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
          speler.getTennisvlaanderenid(), speler.getNaam(), speler.getPunten());
    });
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    return (Speler) jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenid = :nummer")
          .bind("nummer", tennisvlaanderenId)
          .mapToBean(Speler.class)
          .first();
    });
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
              "UPDATE student SET naam = :naam, punten = :punten, WHERE tennisvlaanderenid = :tennisvlaanderenId;")
          .bindBean(speler)
          .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException("Invalid Speler met identification: " + speler.getTennisvlaanderenid());
    }
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle
          .createUpdate(
              "DELETE FROM student WHERE tennisvlaanderenid = :nummer")
          .bind("nummer", tennisvlaanderenid)
          .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException("Invalid Speler met identification: " + tennisvlaanderenid);
    }
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    // TODO: verwijder de "throw new UnsupportedOperationException" en schrijf de code die de gewenste methode op de juiste manier implementeerd zodat de testen slagen.
    throw new UnsupportedOperationException("Unimplemented method 'getHoogsteRankingVanSpeler'");
  }

  @Override
  public void addSpelerToTornooi(int tornooiId) {
    // TODO: verwijder de "throw new UnsupportedOperationException" en schrijf de code die de gewenste methode op de juiste manier implementeerd zodat de testen slagen.
    throw new UnsupportedOperationException("Unimplemented method 'addSpelerToTornooi'");
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId) {
    // TODO: verwijder de "throw new UnsupportedOperationException" en schrijf de code die de gewenste methode op de juiste manier implementeerd zodat de testen slagen.
    throw new UnsupportedOperationException("Unimplemented method 'removeSpelerFromTornooi'");
  }
}
