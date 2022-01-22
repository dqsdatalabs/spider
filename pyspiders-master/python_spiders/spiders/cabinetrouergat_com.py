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

class MySpider(Spider):
    name = 'cabinetrouergat_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cabinetrouergat_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://cabinetrouergat.com/advanced-search/?type=appartement&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://cabinetrouergat.com/advanced-search/?keyword=&status=location&type=maison&bedrooms=&min-area=&max-price=&bathrooms=&max-area=&min-price=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://cabinetrouergat.com/advanced-search/?keyword=&status=location&type=studio&bedrooms=&min-area=&max-price=&bathrooms=&max-area=&min-price=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'property-item')]//figure[@class='item-thumb']"):
            status = item.xpath("./span/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[@rel='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cabinetrouergat_PySpider_france")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//ul/li[@class='detail-city']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        zipcode = response.xpath("//ul/li[@class='detail-zip']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        square_meters = response.xpath("//span/span[contains(.,'Surface')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        rent = response.xpath("//li//strong[contains(.,'Loyer')]/following-sibling::label/text()").get()
        if rent:
            price = rent.split("€")[0].strip()
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//li//strong[contains(.,'Chambre')]/following-sibling::label/text()").get()
        room_count2 = response.xpath("//li//strong[contains(.,'Pièce')]/following-sibling::label/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif room_count2:
            item_loader.add_value("room_count", room_count2.split(" ")[0])
        elif response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
            
        bathroom_count = response.xpath("//li//strong[contains(.,'Salle')]/following-sibling::label/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//li//strong[contains(.,'Etage')]/following-sibling::label/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        desc = "".join(response.xpath("//div[@id='description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='slider-thumbs']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//h5[contains(.,'DPE')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip().split(" ")[0])
        
        external_id = response.xpath("//span/span[contains(.,'Référence')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        deposit = response.xpath("//li//strong[contains(.,'Garantie')]/following-sibling::label/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//strong[contains(.,'Charge')]/following-sibling::label/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        elif response.xpath("//strong[contains(.,'Honoraires à la charge')]/following-sibling::label/text()").get():
            item_loader.add_value("utilities", response.xpath("//strong[contains(.,'Honoraires à la charge')]/following-sibling::label/text()").get().split("€")[0].strip().replace(' ', ''))
        
        elevator = response.xpath("//li//a/i[contains(@class,'check')]/parent::a[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li//i[contains(@class,'check')]//parent::li[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li//i[contains(@class,'check')]//parent::li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        lat_lng = response.xpath("//script[contains(.,'property_lng')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split('"property_lat":"')[1].split('"')[0]
            longitude = lat_lng.split('"property_lng":"')[1].split('"')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
               
        name = response.xpath("//dt[contains(.,'Contact')]/following-sibling::dd[1]/text()").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        
        phone = response.xpath("//dt[contains(.,'Contact')]/following-sibling::dd//i[contains(@class,'phone')]/parent::span//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_email", "stephanalary@cabinetrouergat.com")
        
        yield item_loader.load_item()