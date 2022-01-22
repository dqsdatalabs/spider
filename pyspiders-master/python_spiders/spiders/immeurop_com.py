# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'immeurop_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.immeurop.com/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type" : "house"
            },
            {
                "url" : "https://www.immeurop.com/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='item-photo-link']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[.='Suivante']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            )

        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Immeurop_PySpider_"+ self.country + "_" + self.locale)

        script_map = response.xpath("//script[contains(.,'var latitude =')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("var latitude =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("var longitude =")[1].split(";")[0].strip())

        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/strong/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)
            
        room_count = response.xpath("//li[contains(.,'Nb. de chambres')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//li[contains(.,'Nb. de pièces')]/strong/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
            
        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            rent = rent.strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')            

        external_id = response.xpath("//p[@class='header-ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[1].strip())

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//li[contains(.,'Ville')]/strong/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        item_loader.add_xpath("zipcode","//li[contains(.,'Code postal')]/strong/text()")
        item_loader.add_xpath("utilities","//li[contains(.,'Charges')]/strong/text()")
        item_loader.add_xpath("bathroom_count","//li[contains(.,'Nb. de salle d')]/strong/text()")
        address1 = response.xpath("//li[contains(.,'Département')]/strong/text()").get()
        address = response.xpath("//li[contains(.,'Rue')]/strong/text()").get()
        if city and address1 and address:
            address = address+", "+city+", "+address1
        elif city and address1:
            address = city+", "+address1
        else:
            address = city
        item_loader.add_value("address", address.strip())
        images = [x for x in response.xpath("//div[@id='photoslider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//li[contains(.,'Dépot de garantie')]/strong/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        floor = response.xpath("//li[contains(.,'Etage')]/strong/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        parking = response.xpath("//li[contains(.,'Nb. parking ext') or contains(.,'garage')]/strong/text()").get()
        if parking:
            parking = parking.strip()
            if int(parking) > 0:
                parking = True
                item_loader.add_value("parking", parking)

        elevator = response.xpath("//li[contains(.,'Ascenseur')]/strong/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//li[contains(.,'Balcon')]/strong/text()").get()
        if balcony:
            if balcony.strip().lower() == 'oui':
                balcony = True
            elif balcony.strip().lower() == 'non':
                balcony = False
            if type(balcony) == bool:
                item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[contains(.,'Terrasse')]/strong/text()").get()
        if terrace:
            if terrace.strip().lower() == 'oui':
                terrace = True
            elif terrace.strip().lower() == 'non':
                terrace = False
            if type(terrace) == bool:
                item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//li[contains(.,'Piscine')]/strong/text()").get()
        if swimming_pool:
            if swimming_pool.strip().lower() == 'oui':
                swimming_pool = True
            elif swimming_pool.strip().lower() == 'non':
                swimming_pool = False
            if type(swimming_pool) == bool:
                item_loader.add_value("swimming_pool", swimming_pool)

        item_loader.add_value("landlord_phone", '04 93 26 80 26')
        item_loader.add_value("landlord_name", 'Imm Europ')
        item_loader.add_xpath("title", "//h1/text()")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data