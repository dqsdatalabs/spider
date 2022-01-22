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
import re
from datetime import datetime 
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'remax7tepe'
    execution_type='testing'
    country='turkey'
    locale='tr'
    external_source = "Remax7tepe_PySpider_turkey_tr"


    def start_requests(self):

        start_urls = [
            {"loc" : 10},
            {"loc" : 11},
            {"loc" : 9},
            {"loc" : 6},
        ] #LEVEL-1

        for url in start_urls:
            loc_id = str(url.get("loc"))
            payload = {
                "bolgeID": loc_id,
                "turID": "2",
                "fiyatID": "1",
            }

            yield FormRequest(url="https://www.remax7tepe.com/arama",
                                callback=self.parse,
                                formdata=payload)
                                #headers=self.headers)
            
    # 1. FOLLOWING
    def parse(self, response): 
        for item in response.xpath("//div[@class='listing-item']"):
            href = item.xpath("./a/@href").get()
            property_type = item.xpath(".//div[contains(@class,'title')]/div/text()").get()
            if 'ticari' in property_type.lower():
                property_type = 'pass'
            elif 'daire' in property_type.lower() or 'apartman' in property_type.lower():
                property_type = 'apartment'
            elif 'villa' in property_type.lower():
                property_type = 'house'
            else:
                property_type = 'pass'
            if property_type != 'pass':
                follow_url = response.urljoin(href)
                yield Request(
                    follow_url, 
                    callback=self.populate_item, 
                    meta={"property_type": property_type}
                )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get("property_type"))

        item_loader.add_value("external_link", response.url)
        item_loader.add_css("title", "h1")
        title = response.xpath("//h/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_source", "Remax7tepe_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//td[contains(.,'Bölge')]/following-sibling::td/text()").get()
        if address:
            address = 'İstanbul ' + address.strip()
            item_loader.add_value("address", address)

        square_meters = response.xpath("//td[contains(.,'Alan')]/following-sibling::td/text()").get()
        if square_meters:
            square_meters = square_meters.split(':')[-1].split('m')[0].strip()
            square_meters = square_meters.replace('\xa0', '').replace(',', '.').replace(' ', '.').strip()
            square_meters = str(int(float(square_meters)))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//td[contains(.,'Oda Sayısı')]/following-sibling::td/text()").get()
        if room_count:
            if len(room_count.split('+')) > 1:
                room_count = str(int(float(room_count.split('+')[0].strip()) + float(room_count.split('+')[1].strip())))
            else:
                room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//td[contains(.,'Banyo')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = "".join(response.xpath("//td[contains(.,'Fiyat')]/following-sibling::td/text()").getall())
        if rent:
            price = rent.split("₺")[1].replace(".","").strip()
            item_loader.add_value("rent", price.strip())
            item_loader.add_value("currency", 'TRY')
            
        external_id = response.xpath("//td[contains(.,'Portföy No')]/following-sibling::td/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//h4[.='Detaylar']/following-sibling::p//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        if "balkon" in desc_html.lower():
            item_loader.add_value("balcony", True)
        
        floor=response.xpath("//td[contains(.,'Kat')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        city = response.xpath("//td[contains(.,'Bölge')]/following-sibling::td/text()").get()
        if city:
            city = city.strip()
            item_loader.add_value("city", city)

        images = [urljoin('https://www.remax7tepe.com' ,x) for x in response.xpath("//div[@id='thumbGalleryDetail']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        utilities = response.xpath("//td[contains(.,'Aidat')]/following-sibling::td/text()").get()
        if utilities:
            utilities = utilities.strip()
            item_loader.add_value("utilities", utilities)

        parking = response.xpath("//li[contains(.,'Otopark')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//li[contains(.,'Asansör')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)
        
        swimming_pool = response.xpath("//li[contains(.,'Havuz')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        landlord_name = response.xpath("//span[contains(@class,'agent-infos')]/strong/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//span[contains(@class,'agent-infos')]/span[@class='active_']/span[1]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//span[contains(@class,'agent-infos')]/span[@class='active_']/span[2]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        status=response.xpath("//h1/text()").get()
        if "SAYFA BULUNAMADI!" not in status:
            yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data