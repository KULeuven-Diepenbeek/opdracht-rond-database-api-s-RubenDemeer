package be.kuleuven;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

public class SpelerRepositoryJPAimpl implements SpelerRepository {
  private final EntityManager em;
  public static final String PERSISTANCE_UNIT_NAME = "be.kuleuven.spelerhibernateTest";

  // Constructor
  SpelerRepositoryJPAimpl(EntityManager entityManager) {
    this.em = entityManager;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    em.getTransaction().begin();
    if (em.find(Speler.class, speler.getTennisvlaanderenId()) != null) {
      em.getTransaction().rollback();
      throw new RuntimeException(" A PRIMARY KEY constraint failed");
    }
    em.persist(speler);
    em.getTransaction().commit();
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler speler = em.find(Speler.class, tennisvlaanderenId);
    if (speler == null) {
      throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));
    }
    return speler;
  }

  @Override
  public List<Speler> getAllSpelers() {
    CriteriaBuilder cb = em.getCriteriaBuilder();
    CriteriaQuery<Speler> cq = cb.createQuery(Speler.class);
    Root<Speler> root = cq.from(Speler.class);
    cq.select(root);
    return em.createQuery(cq).getResultList();
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    em.getTransaction().begin();
    if (em.find(Speler.class, speler.getTennisvlaanderenId()) == null) {
      throw new InvalidSpelerException(String.valueOf(speler.getTennisvlaanderenId()));
    }
    em.merge(speler);
    em.getTransaction().commit();
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenId) {
    em.getTransaction().begin();
    Speler speler = em.find(Speler.class, tennisvlaanderenId);
    if (speler == null) {
      throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));
    }
    em.remove(speler);
    em.getTransaction().commit();
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenId) {
    Speler speler = getSpelerByTennisvlaanderenId(tennisvlaanderenId);
    int speler1 = tennisvlaanderenId;
    int speler2 = tennisvlaanderenId;
    String clubnaam = null;
    String finaleString = null;
    
    CriteriaBuilder cb = em.getCriteriaBuilder();
    CriteriaQuery<Wedstrijd> cq = cb.createQuery(Wedstrijd.class);
    Root<Wedstrijd> root = cq.from(Wedstrijd.class);
    cq.select(root);
    cq.where(cb.or(
      cb.equal(root.get("speler1"), speler1),
      cb.equal(root.get("speler2"), speler2)
    ));
    List<Wedstrijd> wedstrijden = em.createQuery(cq).getResultList();
    for (Wedstrijd wedstrijd : wedstrijden) {
        speler.addWedstrijd(wedstrijd);
    }
    Wedstrijd wedstrijd = (Wedstrijd) wedstrijden.stream()
        .filter(w -> w.getWinnaarId() == speler.getTennisvlaanderenId() && w.getFinale() == 1)
        .findAny()
        .orElse(wedstrijden.stream().min(Comparator.comparingInt(Wedstrijd::getFinale)).orElse(null));
    
    if (wedstrijd != null) {
      switch (wedstrijd.getFinale()) {
        case 1:
          if (wedstrijd.getWinnaarId() == speler.getTennisvlaanderenId()) {
            finaleString = "winst";
          } else {
            finaleString = "finale";
          }
          
        case 2:
          finaleString = "halve finale";
          
        case 4:
          finaleString = "kwart finale";
          
        default:
          finaleString ="groepsfase";
          
      }
      int tornooiId = wedstrijd.getTornooiId();
      Tornooi tornooi = em.find(Tornooi.class, tornooiId);
      if (tornooi != null) {
        clubnaam = tornooi.getClubnaam();
      } else {
        throw new InvalidTornooiException("Tornooi not found for ID: " + tornooiId);
      }
    }
    return "Hoogst geplaatst in het tornooi van " + clubnaam + " met plaats in de " + finaleString;
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    EntityTransaction tx = em.getTransaction();
    try {
      tx.begin();

      Speler speler = em.find(Speler.class, tennisvlaanderenId);
      Tornooi tornooi = em.find(Tornooi.class, tornooiId);

      speler.getTornooien().add(tornooi);
      em.merge(speler);

      tx.commit();
    } catch (Exception e) {
      if (tx.isActive())
        tx.rollback();
      throw e;
    } finally {
      em.close();
    }
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    EntityTransaction tx = em.getTransaction();

    try {
      tx.begin();

      Speler speler = em.find(Speler.class, tennisvlaanderenId);
      Tornooi tornooi = em.find(Tornooi.class, tornooiId);

      if (speler == null || tornooi == null) {
        throw new IllegalArgumentException("Speler or Tornooi not found");
      }

      speler.getTornooien().remove(tornooi);
      em.merge(speler);

      tx.commit();
    } catch (Exception e) {
      if (tx.isActive())
        tx.rollback();
      throw e;
    } finally {
      em.close();
    }
  }
}