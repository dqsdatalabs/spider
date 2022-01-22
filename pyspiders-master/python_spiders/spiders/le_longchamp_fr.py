# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'le_longchamp_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.le-longchamp.fr/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.le-longchamp.fr/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//p[@class='lien-detail']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='Suivante']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div[@class='description']/h2/text()")

        item_loader.add_value("external_source", "Lelongchamp_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//script[contains(.,' latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("latitude = ")[1].split(";")[0]
            longitude = latitude_longitude.split("longitude = ")[1].split(";")[0]            
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            

        square_meters = response.xpath("//strong[contains(.,'Surface totale')]/parent::li/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)        

        room_count = response.xpath("//h3[contains(.,'Pièces')]/following-sibling::ul/li[contains(.,'chambres') or contains(.,'Nb. de pièce')]/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//strong[contains(.,'salle de bain') or contains(.,'Nb. de salle d') ]/parent::li/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())  
        rent = response.xpath("//p[@class='prix with-mention']//span[@itemprop='price']/text()").get()
        if rent:
            rent = rent.strip().replace(' ', '')
            item_loader.add_value("rent", rent)       

        currency = 'EUR'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//strong[contains(.,'Réf.')]/parent::li/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='description']/p[1]/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//strong[contains(.,'Ville')]/parent::li/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city.strip())

        zipcode = response.xpath("//strong[contains(.,'Code postal')]/parent::li/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        available_date = response.xpath("//strong[contains(.,'Disponibilité')]/parent::li/text()").get()
        if available_date:
            available_date = available_date.strip()
            if available_date.isalpha() != True:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@id='photoslider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//strong[contains(.,'Dépot de garantie')]/parent::li/text()[2]").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace(' ', '')
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//strong[contains(.,'Charges')]/parent::li/text()[2]").get()
        if utilities:
            utilities = utilities.split('€')[0].strip().replace(' ', '')
            item_loader.add_value("utilities", utilities.strip())

        floor = response.xpath("//strong[contains(.,'Etage')]/parent::li/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//strong[contains(.,'parking')]/parent::li/text()").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//strong[contains(.,'Ascenseur')]/parent::li/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//strong[contains(.,'Balcon')]/parent::li/text()").get()
        if balcony:
            if balcony.strip().lower() == 'oui':
                balcony = True
            elif balcony.strip().lower() == 'non':
                balcony = False
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//strong[contains(.,'Terrasse')]/parent::li/text()").get()
        if terrace:
            if terrace.strip().lower() == 'oui':
                terrace = True
            elif terrace.strip().lower() == 'non':
                terrace = False
            item_loader.add_value("terrace", terrace)

        furnished = response.xpath("//strong[contains(.,'Meublé')]/parent::li/text()").get()
        if furnished:
            if furnished.strip().lower() == 'oui':
                furnished = True
            elif furnished.strip().lower() == 'non':
                furnished = False
            item_loader.add_value("furnished", furnished)

        item_loader.add_value("landlord_phone","04 76 46 58 72")
        item_loader.add_value("landlord_name", "Le Longchamp")
        item_loader.add_value("landlord_email", "accueil@lelongchamp.immo")

        energy_label = response.xpath("//div[@class='diagnostic_images']//img[not(contains(@data-src,'ges'))]/@data-src[not(contains(.,'non'))]").get()
        if energy_label:
            energy_label = energy_label.split('dpe/')[1].split('/')[0].strip()
            if energy_label.isnumeric():
                if int(energy_label) <= 50:
                    energy_label = 'A'
                elif 50 < int(energy_label) and int(energy_label) <= 90:
                    energy_label = 'B'
                elif 90 < int(energy_label) and int(energy_label) <= 150:
                    energy_label = 'C'
                elif 150 < int(energy_label) and int(energy_label) <= 230:
                    energy_label = 'D'
                elif 230 < int(energy_label) and int(energy_label) <= 330:
                    energy_label = 'E'
                elif 330 < int(energy_label) and int(energy_label) <= 450:
                    energy_label = 'F'
                elif 450 < int(energy_label):
                    energy_label = 'G'
                if energy_label.isalpha():
                    item_loader.add_value("energy_label", energy_label)
             
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data
        
       

        
        
          

        

      
     