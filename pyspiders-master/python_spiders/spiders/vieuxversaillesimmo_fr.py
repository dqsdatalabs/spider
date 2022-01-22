# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'vieuxversaillesimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "http://www.vieuxversaillesimmo.fr/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&px_loyermax=Max&px_loyermin=Min&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&surfacemax=Max&surfacemin=Min&tri=d_dt_crea&", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='recherche-resultats-listing']/div//div[contains(@class,'recherche-annonces-location')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = "".join(response.xpath("//h1//text()").extract())
        title2 =  title.strip().replace('\r', '').replace('\n', '')
        item_loader.add_value("title", re.sub("\s{2,}", " ", title2))
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Vieuxversaillesimmo_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//script[contains(.,'LATITUDE')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('MAP_CENTER_LATITUDE: "')[1].split('"')[0].strip().replace(',', '.')
            longitude = latitude_longitude.split('MAP_CENTER_LONGITUDE: "')[1].split('"')[0].strip().replace(',', '.')

            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        address = "".join(response.xpath("//h1[@itemprop ='name']/text()").extract())
        if address:
            if "-" in address:
                add = address.strip().split("-")[1]
                item_loader.add_value("address", add)
                item_loader.add_value("city", add.split("(")[0].strip())
                item_loader.add_value("zipcode", add.split("(")[1].split(")")[0].strip())

        square_meters = response.xpath("//div[.='Surface']/following-sibling::div/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[.='Chambre']/following-sibling::div/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0] 
            item_loader.add_value("room_count", room_count) 
        else:
            room_count = response.xpath("//div[.='Chambres']/following-sibling::div/text()").get()
            if room_count:
                room_count = room_count.strip().split(' ')[0]
                item_loader.add_value("room_count", room_count)
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room1=response.xpath("//div[.='Pièce']/following-sibling::div/text()").get()
            if room1:
                room1= room1.strip().split(' ')[0]
                item_loader.add_value("room_count", room1)

        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            rent = rent.strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        utilities = response.xpath("normalize-space(//ul[@class='margin-top-neg-7']/li[contains(.,'Charges')]/text())").get()
        if utilities:
            utilities = utilities.strip().split(":")[1]
            item_loader.add_value("utilities", utilities)

        external_id = response.xpath("//span[contains(.,'Référence')]/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        bathroom_count = "".join(response.xpath("//li[contains(@title,'Salle d')]/div[2]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        furnished = response.xpath("//li[contains(@title,'Meublé')]/div[2]/text()[contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        # city = response.xpath("//h1[@itemprop='name']/text()[2]").get()
        # if city:
        #     city = city.split('(')[0].strip()
        #     item_loader.add_value("city", city)

        images = [x for x in response.xpath("//div[@id='slider']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//strong[contains(.,'Dépôt de garantie')]/text()[not(contains(.,'N/A'))]").get()
        if deposit:
            deposit = str(int(float(deposit.split(':')[1].split('€')[0].strip().replace(' ', '').replace(',', '.'))))
            if deposit != 'N/A':
                item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//p[.='Consommations énergétiques']/parent::div//div[contains(@class,'dpe-bloc-lettre')]/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            if energy_label != 'VI':
                item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//div[.='Etage']/following-sibling::div/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//div[.='Ascenseur']/following-sibling::div/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)
        
        balcony = response.xpath("//div[.='Balcon']/following-sibling::div/text()").get()
        if balcony:
            if int(balcony.strip()) > 0:
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//div[.='Terrasse']/following-sibling::div/text()").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        landlord_name = response.xpath("//div[@id='detail-agence-nom']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='margin-top-10']//span[@id='numero-telephonez-nous-detail']/text()[2]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        item_loader.add_value("landlord_email", "vieuxversaillesimmo@wanadoo.fr")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data