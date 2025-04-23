package be.kuleuven;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SpelerRepositoryJDBCimpl implements SpelerRepository {
  private Connection connection;

  // Constructor
  SpelerRepositoryJDBCimpl(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    try {
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("INSERT INTO speler (tennisvlaanderenid, naam, punten) VALUES (?, ?, ?);");
      prepared.setInt(1, speler.getTennisvlaanderenId()); // First questionmark
      prepared.setString(2, speler.getNaam()); // Second questionmark
      prepared.setInt(3, speler.getPunten()); // Third questionmark
      prepared.executeUpdate();

      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler found_speler = null;
    try {
      Statement s = (Statement) connection.createStatement();
      String stmt = "SELECT * FROM speler WHERE tennisvlaanderenid = '" + tennisvlaanderenId + "'";
      ResultSet result = s.executeQuery(stmt);

      while (result.next()) {
        int tennisvlaanderenIdFromDb = result.getInt("tennisvlaanderenid");
        String naam = result.getString("naam");
        int punten = result.getInt("punten");
        found_speler = new Speler (tennisvlaanderenIdFromDb, naam, punten);
      }
      if (found_speler == null) {
        throw new InvalidSpelerException("Invalid Speler met identification: " + tennisvlaanderenId + "");
      }
      
      // tornooien
      stmt = "SELECT v.* FROM speler_speelt_tornooi svv JOIN tornooi v ON svv.tornooi = v.id WHERE svv.speler = "+ found_speler.getTennisvlaanderenId() +";";
      result = s.executeQuery(stmt);
      while (result.next()) {
        int tornooiId = result.getInt("id");
        String clubnaam = result.getString("clubnaam");
        found_speler.addTornooi(new Tornooi(tornooiId, clubnaam));
      }


      // wedstrijden
      for(int i = 1; i < 3; i++){ // speler1, speler2
        stmt = "SELECT * FROM wedstrijd WHERE speler"+i+" = "+ found_speler.getTennisvlaanderenId() +";";
        result = s.executeQuery(stmt);
        while (result.next()) {
          int wedstrijdId = result.getInt("id");
          int tornooiId = result.getInt("tornooi");
          int speler1Id = result.getInt("speler1");
          int speler2Id = result.getInt("speler2");
          int winnaarId = result.getInt("winnaar");
          String score = result.getString("score");
          int finale = result.getInt("finale");
          found_speler.addWedstrijd(new Wedstrijd(wedstrijdId, tornooiId, speler1Id, speler2Id, winnaarId, score, finale));
        }
      }

      result.close();
      s.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return found_speler;
  }

  @Override
  public List<Speler> getAllSpelers() {
    // TODO: verwijder de "throw new UnsupportedOperationException" en schrijf de code die de gewenste methode op de juiste manier implementeerd zodat de testen slagen.
    ArrayList<Speler> resultList = new ArrayList<Speler>();
    try {
      Statement s = (Statement) connection.createStatement();
      String stmt = "SELECT * FROM speler";
      ResultSet result = s.executeQuery(stmt);

      while (result.next()) {
        int studnr = result.getInt("tennisvlaanderenid");
        String naam = result.getString("naam");
        int punten = result.getInt("punten");
        resultList.add(new Speler(studnr, naam, punten));
      }
      result.close();
      s.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return resultList;
  }
  

  @Override
  public void updateSpelerInDb(Speler speler) {
    getSpelerByTennisvlaanderenId(speler.getTennisvlaanderenId()); // Check if speler exists
    try {
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("UPDATE speler SET naam = ?, punten = ? WHERE tennisvlaanderenid = ?;");
      prepared.setString(1, speler.getNaam()); // First questionmark
      prepared.setInt(2, speler.getPunten()); // Second questionmark
      prepared.setInt(3, speler.getTennisvlaanderenId()); // Third questionmark
      prepared.executeUpdate();
      prepared.close();
      connection.commit();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    getSpelerByTennisvlaanderenId(tennisvlaanderenid); // Check if speler exists
    try {
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("DELETE FROM speler WHERE tennisvlaanderenid = ?;");
      prepared.setInt(1, tennisvlaanderenid); // First questionmark
      prepared.executeUpdate();
      prepared.close();
      connection.commit();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    Speler speler = getSpelerByTennisvlaanderenId(tennisvlaanderenid); // Check if speler exists
    String resultString = null;
    
    try{
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("SELECT t.clubnaam, w.finale, w.winnaar " +
          "FROM wedstrijd w " +
          "JOIN tornooi t ON w.tornooi = t.id " + 
          "WHERE (speler1 = ? OR speler2 = ?) " +
          "ORDER BY w.finale DESC; ");
      prepared.setInt(1, speler.getTennisvlaanderenId()); // First questionmark
      prepared.setInt(2, speler.getTennisvlaanderenId()); // Second questionmark
      ResultSet result = prepared.executeQuery();
      String finaleString = null;
      String clubnaam = null;
      while (result.next()) {
        clubnaam = result.getString("clubnaam");
        int finale = result.getInt("finale");
        int winnaar = result.getInt("winnaar");
        if(finale == 1 && winnaar == tennisvlaanderenid){
          finaleString = "winst";
          break;
        }else if(finale == 1){
          finaleString = "finale";
        }else if(finale == 2){
          finaleString = "halve finale";
        }else if(finale == 4){
          finaleString = "kwart finale";
        }else{
          finaleString = "lager dan kwart finale";
        }
        
      }
      resultString = "Hoogst geplaatst in het tornooi van " + clubnaam + " met plaats in de " + finaleString;
      result.close();
      prepared.close();
      connection.commit();
    }catch(Exception e){
      throw new RuntimeException(e);
    }
    return resultString;
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    try {
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (?, ?);");
      prepared.setInt(1, tennisvlaanderenId); // First questionmark
      prepared.setInt(2, tornooiId); // Second questionmark
      prepared.executeUpdate();
      prepared.close();
      connection.commit();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    try {
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("DELETE FROM speler_speelt_tornooi WHERE speler = ? AND tornooi = ?;");
      prepared.setInt(1, tennisvlaanderenId); // First questionmark
      prepared.setInt(2, tornooiId); // Second questionmark
      prepared.executeUpdate();
      prepared.close();
      connection.commit();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
