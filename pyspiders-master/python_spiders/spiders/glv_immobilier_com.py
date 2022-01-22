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
import re

class MySpider(Spider):
    name = 'glv_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = {
        "PROXY_ON":"True"
    }
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "http://www.glv-immobilier.fr/recherche/?transaction=location&type%5B%5D=appartement&reference=",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.glv-immobilier.fr/recherche/?transaction=location&type%5B%5D=maison&reference=",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        
        if response.xpath("//a[@class='link_detail']/@href").extract():
            for item in response.xpath("//a[@class='link_detail']/@href").extract():
                f_url = response.urljoin(item)
                yield Request(
                    f_url, 
                    callback=self.populate_item, 
                    meta={"property_type" : response.meta.get("property_type")},
                )
                seen=True
        else:
            sel = Selector(text=response.body, type='html')
            for item in sel.xpath("//a[contains(@class,'link_detail')]/@href").extract():
                item = item.replace("\/","/").split("detail/")[1].split("/")[0]
                url = f"https://www.glv-immobilier.fr/detail/{item}/"
                yield Request(
                    url, 
                    callback=self.populate_item, 
                    meta={"property_type" : response.meta.get("property_type")},
                )
                seen=True

        if page ==2 or seen:    
            if response.meta.get("property_type") == "apartment":
                f_url = f"https://www.glv-immobilier.fr/scrolling-annonces/?undefined%5B%5D=appartement&type%5B%5D=appartement&transaction=location&page={page}"
                yield Request(f_url, callback=self.parse, meta={"page": page+1, "property_type" : response.meta.get("property_type")})
            if response.meta.get("property_type") == "house":
                f_url = f"https://www.glv-immobilier.fr/scrolling-annonces/?undefined%5B%5D=maison&type%5B%5D=maison&transaction=location&page={page}"
                yield Request(f_url, callback=self.parse, meta={"page": page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Glvimmobilier_PySpider_"+ self.country + "_" + self.locale)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            
        address = response.xpath("//span[@class='uppercase']/text()").get()
        if address:
            item_loader.add_value('address', address.strip())
            if '-' in address:
                city = address.split()[0]
            else:
                city = address.split()[-1]
            item_loader.add_value('city', city)
        square_meters = response.xpath("//li[contains(.,'Superficie')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            square_meters = square_meters.replace('\xa0', '').replace(',', '.').replace(' ', '.').strip()
            square_meters = str(int(float(square_meters)))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(.,'Nombre de chambres')]/span/text()").get()
        if room_count and "0" not in room_count:
            room_count = room_count.strip().replace('\xa0', '')
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'pièce')]/span/text()").get()
            if room_count and "0" not in room_count:
                room_count = room_count.strip().replace('\xa0', '')
                room_count = str(int(float(room_count)))
                item_loader.add_value("room_count", room_count)
            
        rent = response.xpath("//h1/following-sibling::span[@class='price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '')
            rent = rent.replace(',', '').replace('.', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//h1/span[contains(.,'Réf')]/text()").get()
        if external_id:
            external_id = external_id.split('.')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//span[@class='infos_text']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [x for x in response.xpath("//div[@class='carousel_detail']/a/@href[not(contains(.,'youtube'))]").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        utilities = response.xpath("//li[contains(.,'charges locatives')]/span/text()").get()
        if utilities:
            utilities = utilities.split('€')[0].strip().replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/span/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//span[contains(@class,'dpeInfoConso')]/span/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//li[contains(.,'Étage')]/span/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        landlord_name = response.xpath("//div[@class='agent_name']/text()").get()
        landlord_name2 = response.xpath("//div[@class='agent_name']/span/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            if landlord_name2:
                landlord_name += ' ' + landlord_name2.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='agent_phone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        item_loader.add_value("landlord_email", "contact@glv-immobilier.fr")
        
        status = " ".join(response.xpath("//span[@class='infos_text' and contains(.,'LOUÉ PAR LE CABINET')]//text()").getall())
        if not status:
            yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data