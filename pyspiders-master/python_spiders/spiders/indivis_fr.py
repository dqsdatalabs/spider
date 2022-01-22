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
import re

class MySpider(Spider):
    name = 'indivis_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Indivis_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {"url": "https://www.indivis.fr/advanced-search/?keyword=&status=location&type=appartement&bedrooms=&min-area=&max-area=&min-price=&max-price=", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for follow_url in response.xpath("//div[contains(@class,'property-listing')]/div//h2/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        next_page = response.xpath("//a[contains(@rel,'Next')]//@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        
        
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        
        address=response.xpath("//ul/li/strong[contains(.,'Adresse')]/parent::li/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        city=response.xpath("//ul/li[@class='detail-city']/text()").get()
        if city:
            item_loader.add_value("city", city.strip().split(" ")[0])
            
        zipcode=response.xpath("//ul/li/strong[contains(.,'Code')]/parent::li/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'property_lat')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('property_lat":"')[1].split('"')[0].strip()
            longitude = latitude_longitude.split('property_lng":"')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square_meters = response.xpath("//strong[contains(.,'urface')]/parent::div/label/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//strong[contains(.,'hambre') or contains(.,'Pièces')]/parent::div/label/text()").get()
        if room_count:
            room_count = room_count.strip()
            if "pièce(s)" in room_count:
                room_count = room_count.split("pièce(s)")[0].strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//strong[contains(.,'Bain(s)')]/parent::div/label/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//span[@class='item-price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip()
            item_loader.add_value("rent", rent)

        currency = 'EUR'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//span[contains(.,'rence')]/span[2]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@id='description']/p/span/text()").get()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [x for x in response.xpath("//div[@class='slider-thumbs']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        deposit = response.xpath("//strong[contains(.,'épô')]/parent::div/label/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip()
            item_loader.add_value("deposit", deposit)

        utilities=response.xpath("//strong[contains(.,'Provision sur Charges')]/parent::div/label/text()").get()
        if utilities:
            utilities = utilities.split(' ')[0].replace("€", "").strip()
            item_loader.add_value("utilities", utilities)
        
        floor = response.xpath("//strong[contains(.,'Etage')]/parent::div/label/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        energy_label=response.xpath("//div/@class[contains(.,'DPE DPE')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("DPE-")[1].strip())
        
        furnished=response.xpath("//div[@id='description']/p/span/text()[contains(.,'meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        landlord_name = response.xpath("//dt[.='Contact']/parent::dl/dd[1]/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//dt[.='Contact']/parent::dl/dd[2]/span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            landlord_phone = response.xpath("//i[contains(@class,'fa-phone')]/following-sibling::text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
        

        item_loader.add_value("landlord_email","contact@indivis.fr")
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data