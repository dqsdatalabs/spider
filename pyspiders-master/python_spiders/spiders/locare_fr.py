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

        room_count = response.xpath("//div[contains(text(),'pi??ces')]/following-sibling::div/b/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(text(),'Salle')]/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//div[@class='product-price']//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent", str(int(float(rent.split('???')[0].lower().split('loyer')[-1].strip().replace(' ', '').replace('\xa0', '')))))
            item_loader.add_value("currency", 'EUR')
        
        available_date = response.xpath("//span[contains(.,'Disponible ?? partir du')]/b/text()").get()
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


        deposit = response.xpath("//div[contains(text(),'D??p??t de Garantie')]/following-sibling::div/b/text()").get()
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
 'Saint-??tienne',
 'Toulon',
 'Grenoble',
 'Dijon',
 'N??mes',
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
 'Besan??on',
 'Boulogne-Billancourt',
 'Orl??ans',
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
 'Cr??teil',
 'Dunkirk',
 'Poitiers',
 'Asni??res-sur-Seine',
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
 'B??ziers',
 'La Rochelle',
 'Saint-Maur-des-Foss??s',
 'Cannes',
 'Calais',
 'Saint-Nazaire',
 'M??rignac',
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
 'V??nissieux',
 'Pessac',
 'Troyes',
 'Clichy',
 'Ivry-sur-Seine',
 'Chamb??ry',
 'Lorient',
 'Les Abymes',
 'Montauban',
 'Sarcelles',
 'Niort',
 'Villejuif',
 'Saint-Andr??',
 'Hy??res',
 'Saint-Quentin',
 'Beauvais',
 '??pinay-sur-Seine',
 'Cayenne',
 'Maisons-Alfort',
 'Cholet',
 'Meaux',
 'Chelles',
 'Pantin',
 '??vry',
 'Fontenay-sous-Bois',
 'Fr??jus',
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
 '??vreux',
 'Vincennes',
 'Montrouge',
 'Sevran',
 'Albi',
 'Charleville-M??zi??res',
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
 'Ch??teauroux',
 'Chalon-sur-Sa??ne',
 'Mantes-la-Jolie',
 'Meudon',
 'Saint-Malo',
 'Ch??lons-en-Champagne',
 'Alfortville',
 'S??te',
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
 'Angoul??me',
 'Garges-l??s-Gonesse',
 'Castres',
 'Thionville',
 'Wattrelos',
 'Talence',
 'Saint-Laurent-du-Maroni',
 'Douai',
 'Noisy-le-Sec',
 'Tarbes',
 'Arras',
 'Al??s',
 'La Courneuve',
 'Bourg-en-Bresse',
 'Compi??gne',
 'Gap',
 'Melun',
 'Le Lamentin',
 'Rez??',
 'Saint-Germain-en-Laye',
 'Marcq-en-Bar??ul',
 'Gagny',
 'Anglet',
 'Draguignan',
 'Chartres',
 'Bron',
 'Bagneux',
 'Colomiers',
 "Saint-Martin-d'H??res",
 'Pontault-Combault',
 'Montlu??on',
 'Jou??-l??s-Tours',
 'Saint-Joseph',
 'Poissy',
 'Savigny-sur-Orge',
 'Cherbourg-Octeville',
 'Mont??limar',
 'Villefranche-sur-Sa??ne',
 'Stains',
 'Saint-Beno??t',
 'Bagnolet',
 'Ch??tillon',
 'Le Port',
 'Sainte-Genevi??ve-des-Bois',
 '??chirolles',
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
 'Saint-Rapha??l',
 'Marignane',
 'Romans-sur-Is??re',
 'Montigny-le-Bretonneux',
 'Le Perreux-sur-Marne',
 'Franconville',
 'M??con',
 'Saint-Leu',
 'Cambrai',
 'Ch??tenay-Malabry',
 'Sainte-Marie',
 'Villeneuve-Saint-Georges',
 'Houilles',
 '??pinal',
 'Lens',
 'Li??vin',
 'Les Mureaux',
 'Schiltigheim',
 'La Possession',
 'Meyzieu',
 'Dreux',
 'Nogent-sur-Marne',
 'Plaisir',
 'Mont-de-Marsan',
 'Palaiseau',
 'Ch??tellerault',
 'Goussainville',
 "L'Ha??-les-Roses",
 'Viry-Ch??tillon',
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
 'P??rigueux',
 'Baie-Mahault',
 'Vandoeuvre-l??s-Nancy',
 'Pontoise',
 'Aix-les-Bains',
 'Cachan',
 'Vienne',
 'Thiais',
 'Orange',
 'Saint-M??dard-en-Jalles',
 'Villemomble',
 'Saint-Cloud',
 'Saint-Laurent-du-Var',
 'Yerres',
 'Saint-??tienne-du-Rouvray',
 'Sotteville-l??s-Rouen',
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
 'D??cines-Charpieu',
 'H??nin-Beaumont',
 'Fresnes',
 'Aurillac',
 'Sannois',
 'Vallauris',
 'Illkirch-Graffenstaden',
 'Alen??on',
 '??lancourt',
 'Tournefeuille',
 'B??gles',
 'Gonesse',
 'Oullins',
 'Brunoy',
 'Taverny',
 'Armenti??res',
 'Montfermeil',
 'Rambouillet',
 'Villeparisis',
 'Le Kremlin-Bic??tre',
 'Sucy-en-Brie',
 'Kourou',
 'Montb??liard',
 'Romainville',
 'Cavaillon',
 'Saint-Dizier',
 'Br??tigny-sur-Orge',
 'Saint-S??bastien-sur-Loire',
 'Saintes',
 'La Teste-de-Buch',
 'Villeneuve-la-Garenne',
 'B??thune',
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
 '??tampes',
 'Gradignan',
 'Vernon',
 'Petit-Bourg',
 'Libourne',
 'Abbeville',
 'Rodez',
 "Saint-Ouen-l'Aum??ne",
 'Torcy',
 'Maisons-Laffitte',
 'Montgeron',
 'Villeneuve-sur-Lot',
 'Cormeilles-en-Parisis',
 '??pernay',
 'S??vres',
 'Dole',
 'Le Robert',
 'Le Bouscat',
 'Blagnac',
 'Frontignan',
 'Cenon',
 'Mandelieu-la-Napoule',
 'Vertou',
 'Les Lilas',
 'Bruay-la-Buissi??re',
 'Les Pavillons-sous-Bois',
 'Chaumont',
 'Roissy-en-Brie',
 'Le Moule',
 'Le Petit-Quevilly',
 'Manosque',
 'Saint-Mand??',
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
 'Montigny-l??s-Metz',
 'Hazebrouck',
 'Longjumeau',
 'Sainte-Foy-l??s-Lyon',
 'Forbach',
 'Sarreguemines',
 'Mons-en-Bar??ul',
 'La Valette-du-Var',
 'H??rouville-Saint-Clair',
 'Morsang-sur-Orge',
 'Grande-Synthe',
 'La Celle-Saint-Cloud',
 'Lisieux',
 'Croix',
 'Dammarie-les-Lys',
 'V??lizy-Villacoublay',
 'Wasquehal',
 'Saint-Gratien',
 'Halluin',
 'Neuilly-Plaisance',
 'Montmorency',
 'Dax',
 'Lagny-sur-Marne',
 'Le M??e-sur-Seine',
 'Saint-Genis-Laval',
 'Fleury-les-Aubrais',
 'Loos',
 'Gif-sur-Yvette',
 'Denain',
 'Saint-Di??-des-Vosges',
 'Sainte-Rose',
 'Saint-Michel-sur-Orge']
