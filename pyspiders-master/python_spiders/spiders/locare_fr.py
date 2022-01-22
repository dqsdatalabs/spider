# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'locare_fr'    
    execution_type='testing'
    country = 'france'
    locale = 'fr' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.locare.fr/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_33_tmp=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_65_loc=&C_65_vente=&C_30_MIN=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MAX=&keywords=&C_49_type=NUMBER&C_49_search=COMPRIS&C_49_MIN=&C_41_type=FLAG&C_41_search=EGAL&C_46_type=NUMBER&C_46_search=COMPRIS&C_46_MIN=&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_50_type=NUMBER&C_50_search=COMPRIS&C_50_MIN=",
                ],
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='img-product']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//i[contains(@class,'fa-long-arrow-right')]/../@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split('?')[0])

        item_loader.add_value("external_source", "Locare_PySpider_france")

        external_id = response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        address = response.xpath("//div[@class='product-localisation']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            
        title = " ".join(response.xpath("//div[@class='infos-products-header']/h1//text()").getall()).strip()
        if title:
            item_loader.add_value("title", title.replace('\xa0', ''))

            for i in france_city_list:
                if i.lower() in title.lower():
                    item_loader.add_value("city", i)
                    break

        description = " ".join(response.xpath("//div[@class='product-description']//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        else:
            description = ""
        
        square_meters = response.xpath("//div[contains(text(),'Surface')]/following-sibling::div/b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('m')[0].strip()))))

        room_count = response.xpath("//div[contains(text(),'pièces')]/following-sibling::div/b/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(text(),'Salle')]/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//div[@class='product-price']//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent", str(int(float(rent.split('€')[0].lower().split('loyer')[-1].strip().replace(' ', '').replace('\xa0', '')))))
            item_loader.add_value("currency", 'EUR')
        
        available_date = response.xpath("//span[contains(.,'Disponible à partir du')]/b/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        else:
            if "disponible pour le" in description.lower():
                available_date = " ".join(description.lower().split("disponible pour le")[1].strip().split(" ")[0:3])
                date_parsed = dateparser.parse(available_date)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)


        deposit = response.xpath("//div[contains(text(),'Dépôt de Garantie')]/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit", str(int(float(deposit.lower().split('eur')[0].strip().replace(' ', '').replace('\xa0', '')))))
        
        images = [x for x in response.xpath("//div[@id='slider_product']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        latitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip())
        
        energy_label = response.xpath("//div[contains(text(),'Conso Energ')]/following-sibling::div/b/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//div[contains(text(),'Etage')]/following-sibling::div/b/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        utilities = response.xpath("//div[contains(text(),'Provision sur charges')]/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value("utilities", str(int(float(utilities.lower().split('eur')[0].strip().replace(' ', '').replace('\xa0', '')))))
        
        balcony = response.xpath("//div[@class='value' and contains(text(),'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//div[contains(text(),'Ascenseur')]/following-sibling::div/b/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'non':
                item_loader.add_value("elevator", False)
        
        terrace = response.xpath("//div[@class='value' and contains(text(),'terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", 'LOCARE')
        item_loader.add_value("landlord_phone", '08.10.25.25.25')

        yield item_loader.load_item()



france_city_list = ['Paris',
 'Marseille',
 'Lyon',
 'Toulouse',
 'Nice',
 'Nantes',
 'Strasbourg',
 'Montpellier',
 'Bordeaux',
 'Lille',
 'Rennes',
 'Reims',
 'Le Havre',
 'Saint-Étienne',
 'Toulon',
 'Grenoble',
 'Dijon',
 'Nîmes',
 'Angers',
 'Villeurbanne',
 'Le Mans',
 'Saint-Denis',
 'Aix-en-Provence',
 'Clermont-Ferrand',
 'Brest',
 'Limoges',
 'Tours',
 'Amiens',
 'Perpignan',
 'Metz',
 'Besançon',
 'Boulogne-Billancourt',
 'Orléans',
 'Mulhouse',
 'Rouen',
 'Saint-Denis',
 'Caen',
 'Argenteuil',
 'Saint-Paul',
 'Montreuil',
 'Nancy',
 'Roubaix',
 'Tourcoing',
 'Nanterre',
 'Avignon',
 'Vitry-sur-Seine',
 'Créteil',
 'Dunkirk',
 'Poitiers',
 'Asnières-sur-Seine',
 'Courbevoie',
 'Versailles',
 'Colombes',
 'Fort-de-France',
 'Aulnay-sous-Bois',
 'Saint-Pierre',
 'Rueil-Malmaison',
 'Pau',
 'Aubervilliers',
 'Le Tampon',
 'Champigny-sur-Marne',
 'Antibes',
 'Béziers',
 'La Rochelle',
 'Saint-Maur-des-Fossés',
 'Cannes',
 'Calais',
 'Saint-Nazaire',
 'Mérignac',
 'Drancy',
 'Colmar',
 'Ajaccio',
 'Bourges',
 'Issy-les-Moulineaux',
 'Levallois-Perret',
 'La Seyne-sur-Mer',
 'Quimper',
 'Noisy-le-Grand',
 "Villeneuve-d'Ascq",
 'Neuilly-sur-Seine',
 'Valence',
 'Antony',
 'Cergy',
 'Vénissieux',
 'Pessac',
 'Troyes',
 'Clichy',
 'Ivry-sur-Seine',
 'Chambéry',
 'Lorient',
 'Les Abymes',
 'Montauban',
 'Sarcelles',
 'Niort',
 'Villejuif',
 'Saint-André',
 'Hyères',
 'Saint-Quentin',
 'Beauvais',
 'Épinay-sur-Seine',
 'Cayenne',
 'Maisons-Alfort',
 'Cholet',
 'Meaux',
 'Chelles',
 'Pantin',
 'Évry',
 'Fontenay-sous-Bois',
 'Fréjus',
 'Vannes',
 'Bondy',
 'Le Blanc-Mesnil',
 'La Roche-sur-Yon',
 'Saint-Louis',
 'Arles',
 'Clamart',
 'Narbonne',
 'Annecy',
 'Sartrouville',
 'Grasse',
 'Laval',
 'Belfort',
 'Bobigny',
 'Évreux',
 'Vincennes',
 'Montrouge',
 'Sevran',
 'Albi',
 'Charleville-Mézières',
 'Suresnes',
 'Martigues',
 'Corbeil-Essonnes',
 'Saint-Ouen',
 'Bayonne',
 'Cagnes-sur-Mer',
 'Brive-la-Gaillarde',
 'Carcassonne',
 'Massy',
 'Blois',
 'Aubagne',
 'Saint-Brieuc',
 'Châteauroux',
 'Chalon-sur-Saône',
 'Mantes-la-Jolie',
 'Meudon',
 'Saint-Malo',
 'Châlons-en-Champagne',
 'Alfortville',
 'Sète',
 'Salon-de-Provence',
 'Vaulx-en-Velin',
 'Puteaux',
 'Rosny-sous-Bois',
 'Saint-Herblain',
 'Gennevilliers',
 'Le Cannet',
 'Livry-Gargan',
 'Saint-Priest',
 'Istres',
 'Valenciennes',
 'Choisy-le-Roi',
 'Caluire-et-Cuire',
 'Boulogne-sur-Mer',
 'Bastia',
 'Angoulême',
 'Garges-lès-Gonesse',
 'Castres',
 'Thionville',
 'Wattrelos',
 'Talence',
 'Saint-Laurent-du-Maroni',
 'Douai',
 'Noisy-le-Sec',
 'Tarbes',
 'Arras',
 'Alès',
 'La Courneuve',
 'Bourg-en-Bresse',
 'Compiègne',
 'Gap',
 'Melun',
 'Le Lamentin',
 'Rezé',
 'Saint-Germain-en-Laye',
 'Marcq-en-Barœul',
 'Gagny',
 'Anglet',
 'Draguignan',
 'Chartres',
 'Bron',
 'Bagneux',
 'Colomiers',
 "Saint-Martin-d'Hères",
 'Pontault-Combault',
 'Montluçon',
 'Joué-lès-Tours',
 'Saint-Joseph',
 'Poissy',
 'Savigny-sur-Orge',
 'Cherbourg-Octeville',
 'Montélimar',
 'Villefranche-sur-Saône',
 'Stains',
 'Saint-Benoît',
 'Bagnolet',
 'Châtillon',
 'Le Port',
 'Sainte-Geneviève-des-Bois',
 'Échirolles',
 'Roanne',
 'Villepinte',
 'Saint-Chamond',
 'Conflans-Sainte-Honorine',
 'Auxerre',
 'Nevers',
 'Neuilly-sur-Marne',
 'La Ciotat',
 'Tremblay-en-France',
 'Thonon-les-Bains',
 'Vitrolles',
 'Haguenau',
 'Six-Fours-les-Plages',
 'Agen',
 'Creil',
 'Annemasse',
 'Saint-Raphaël',
 'Marignane',
 'Romans-sur-Isère',
 'Montigny-le-Bretonneux',
 'Le Perreux-sur-Marne',
 'Franconville',
 'Mâcon',
 'Saint-Leu',
 'Cambrai',
 'Châtenay-Malabry',
 'Sainte-Marie',
 'Villeneuve-Saint-Georges',
 'Houilles',
 'Épinal',
 'Lens',
 'Liévin',
 'Les Mureaux',
 'Schiltigheim',
 'La Possession',
 'Meyzieu',
 'Dreux',
 'Nogent-sur-Marne',
 'Plaisir',
 'Mont-de-Marsan',
 'Palaiseau',
 'Châtellerault',
 'Goussainville',
 "L'Haÿ-les-Roses",
 'Viry-Châtillon',
 'Vigneux-sur-Seine',
 'Chatou',
 'Trappes',
 'Clichy-sous-Bois',
 'Rillieux-la-Pape',
 "Villenave-d'Ornon",
 'Maubeuge',
 'Charenton-le-Pont',
 'Malakoff',
 'Matoury',
 'Dieppe',
 'Athis-Mons',
 'Savigny-le-Temple',
 'Périgueux',
 'Baie-Mahault',
 'Vandoeuvre-lès-Nancy',
 'Pontoise',
 'Aix-les-Bains',
 'Cachan',
 'Vienne',
 'Thiais',
 'Orange',
 'Saint-Médard-en-Jalles',
 'Villemomble',
 'Saint-Cloud',
 'Saint-Laurent-du-Var',
 'Yerres',
 'Saint-Étienne-du-Rouvray',
 'Sotteville-lès-Rouen',
 'Draveil',
 'Le Chesnay',
 'Bois-Colombes',
 'Le Plessis-Robinson',
 'La Garenne-Colombes',
 'Lambersart',
 'Soissons',
 'Pierrefitte-sur-Seine',
 'Carpentras',
 'Villiers-sur-Marne',
 'Vanves',
 'Menton',
 'Bergerac',
 'Ermont',
 'Bezons',
 'Grigny',
 'Guyancourt',
 'Saumur',
 'Herblay',
 'Ris-Orangis',
 'Villiers-le-Bel',
 'Bourgoin-Jallieu',
 'Vierzon',
 'Le Gosier',
 'Décines-Charpieu',
 'Hénin-Beaumont',
 'Fresnes',
 'Aurillac',
 'Sannois',
 'Vallauris',
 'Illkirch-Graffenstaden',
 'Alençon',
 'Élancourt',
 'Tournefeuille',
 'Bègles',
 'Gonesse',
 'Oullins',
 'Brunoy',
 'Taverny',
 'Armentières',
 'Montfermeil',
 'Rambouillet',
 'Villeparisis',
 'Le Kremlin-Bicêtre',
 'Sucy-en-Brie',
 'Kourou',
 'Montbéliard',
 'Romainville',
 'Cavaillon',
 'Saint-Dizier',
 'Brétigny-sur-Orge',
 'Saint-Sébastien-sur-Loire',
 'Saintes',
 'La Teste-de-Buch',
 'Villeneuve-la-Garenne',
 'Béthune',
 'Bussy-Saint-Georges',
 'Vichy',
 'La Garde',
 'Agde',
 'Laon',
 'Sens',
 'Lunel',
 'Miramas',
 'Biarritz',
 'Le Grand-Quevilly',
 'Orvault',
 'Les Ulis',
 'Champs-sur-Marne',
 'Rochefort',
 'Muret',
 'Sainte-Anne',
 'Eaubonne',
 'Étampes',
 'Gradignan',
 'Vernon',
 'Petit-Bourg',
 'Libourne',
 'Abbeville',
 'Rodez',
 "Saint-Ouen-l'Aumône",
 'Torcy',
 'Maisons-Laffitte',
 'Montgeron',
 'Villeneuve-sur-Lot',
 'Cormeilles-en-Parisis',
 'Épernay',
 'Sèvres',
 'Dole',
 'Le Robert',
 'Le Bouscat',
 'Blagnac',
 'Frontignan',
 'Cenon',
 'Mandelieu-la-Napoule',
 'Vertou',
 'Les Lilas',
 'Bruay-la-Buissière',
 'Les Pavillons-sous-Bois',
 'Chaumont',
 'Roissy-en-Brie',
 'Le Moule',
 'Le Petit-Quevilly',
 'Manosque',
 'Saint-Mandé',
 'Fontenay-aux-Roses',
 'Orly',
 'Le Creusot',
 'Oyonnax',
 'La Madeleine',
 'Sainte-Suzanne',
 'Millau',
 'Combs-la-Ville',
 'Fontaine',
 'Deuil-la-Barre',
 'Coudekerque-Branche',
 'Auch',
 'Lanester',
 'Beaune',
 'Montigny-lès-Metz',
 'Hazebrouck',
 'Longjumeau',
 'Sainte-Foy-lès-Lyon',
 'Forbach',
 'Sarreguemines',
 'Mons-en-Barœul',
 'La Valette-du-Var',
 'Hérouville-Saint-Clair',
 'Morsang-sur-Orge',
 'Grande-Synthe',
 'La Celle-Saint-Cloud',
 'Lisieux',
 'Croix',
 'Dammarie-les-Lys',
 'Vélizy-Villacoublay',
 'Wasquehal',
 'Saint-Gratien',
 'Halluin',
 'Neuilly-Plaisance',
 'Montmorency',
 'Dax',
 'Lagny-sur-Marne',
 'Le Mée-sur-Seine',
 'Saint-Genis-Laval',
 'Fleury-les-Aubrais',
 'Loos',
 'Gif-sur-Yvette',
 'Denain',
 'Saint-Dié-des-Vosges',
 'Sainte-Rose',
 'Saint-Michel-sur-Orge']
