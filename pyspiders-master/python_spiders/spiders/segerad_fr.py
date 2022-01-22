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
    name = 'segerad_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Segerad_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://segerad.fr/proprietes/?type=maison",],
                "property_type" : "house"
            },
            {
                "url" : ["https://segerad.fr/proprietes/?type=studio",],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://segerad.fr/proprietes/?type=t1",
                    "https://segerad.fr/proprietes/?type=t1-bis",
                    "https://segerad.fr/proprietes/?type=t2",
                    "https://segerad.fr/proprietes/?type=t2-meuble",
                    "https://segerad.fr/proprietes/?type=t3",
                    "https://segerad.fr/proprietes/?type=t4",
                    "https://segerad.fr/proprietes/?type=t6",
                ],
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(url=item,
                                    callback=self.parse,
                                    meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        
        for item in response.xpath("//figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        latitude_longitude = response.xpath("//script[contains(.,'map')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('"lang":"')[1].split('"')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        title = response.xpath("//h1[@class='page-title']//text()").get()
        if title:    
            item_loader.add_value("title", title)
        address = response.xpath("//strong[contains(., 'Proximité')]/following-sibling::span/text()").get()
        if address:  
            item_loader.add_value("address", address) 
        elif not address:
            address = response.xpath("//h1[@class='page-title']//text()").get()
            if address:   
                if "/" in address:
                    address = address.split("/")[0].strip()
                elif "–" in address:
                    address = address.split("–")[0].strip()
                item_loader.add_value("address", address)    
                
        square_meters = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'m2')]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)
        else:
            square_meters = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'m²')]/text()").get()
            if square_meters:
                square_meters = square_meters.strip().split('m')[0].strip()
                item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Chambre')]/text()").get()
        if room_count:
            room_count = room_count.strip().split('Ch')[0].strip()
            item_loader.add_value("room_count", room_count)
        elif not room_count:
            if "studio" in response.meta.get('property_type'):
                item_loader.add_value("room_count", "1") 
        
        utilities = response.xpath("//p/text()[contains(.,'CHARGES') or contains(.,'charges')]").get()
        if utilities:
            utilities = utilities.lower().split("charges")[1].replace(":","").split("€")[0].replace(" ","")
            if utilities and utilities != "0":
                item_loader.add_value("utilities", utilities)

        bathroom_count = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Sanitaire')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split('Sanitaire')[0].strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//h5[@class='price']/span[2]/text()[1]").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
            # rent = rent.split('€')[0].strip().replace(' ', '')
            # currency = 'EUR'
            # item_loader.add_value("currency", currency)
       
        external_id = response.xpath("//h4[@class='title']/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='content clearfix']/p//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        available_date = response.xpath("//strong[contains(., 'Disponible')]/following-sibling::span/text()").get()
        if available_date:
            available_date = available_date.replace("A partir du","").strip()
            if available_date.isalpha() != True:
                date_parsed = None
                if 'de suite' in available_date.lower():
                    date_parsed = dateparser.parse('now', date_formats=["%d/%m/%Y"], languages=['en'])
                else:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//ul[@class='slides']//a/@href").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//strong[contains(., 'Dépôt de garantie')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip()
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))

        energy_label = response.xpath("//strong[contains(., 'DPE')]/following-sibling::span/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//strong[contains(., 'Etage')]/following-sibling::span/text()").get()
        if floor:
            floor = floor.split('/')[0]
            item_loader.add_value("floor", floor)

        parking = response.xpath("//strong[contains(., 'Stationnement')]/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//strong[contains(., 'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//strong[contains(., 'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//strong[contains(., 'Terrase') or contains(., 'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_phone", "03 80 30 33 39")
        item_loader.add_value("landlord_email", 'location-gerance@segerad.fr')
        item_loader.add_value("landlord_name", 'SEGERAD')
        
     
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data