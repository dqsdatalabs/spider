# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'agenceavril_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agenceavril.com/en/search/", "property_type": "apartment", "type":"1"},
            {"url": "https://www.agenceavril.com/en/search/", "property_type": "house", "type":"2"},
        ]  # LEVEL 1
        
        for url in start_urls:

            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://www.agenceavril.com",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36"
            }

            data = {
                "nature": "2",
                "type[]": url.get("type"),
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",  
            }

            yield FormRequest(
                url=url.get("url"),
                formdata=data,
                headers=headers,
                callback=self.parse,
                meta={'property_type': url.get('property_type'),
                        "type":url.get('type')}
            )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[@class='ads']/li//a[contains(.,'Detailed')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        pagination = response.xpath("//ul[@class='pager']/li[@class='nextpage']/a/@href").extract_first()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Agenceavril_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[@class='titles']/h1/text()").get()
        item_loader.add_value("title", title)

        city = response.xpath("//span[@class='selectionLink ']/../h2/br/following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city.strip())
      
        item_loader.add_value("external_link", response.url)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('L.marker([')[1].split(']')[0]
            latitude = latitude_longitude.split(',')[0].strip()
            longitude = latitude_longitude.split(',')[1].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        address=response.xpath("normalize-space(//p[contains(@class,'address')]/text())").get()
        if address:
            item_loader.add_value("address", address)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(.,'Rooms')]/span/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//span[@class='selectionLink ']/parent::div/ul/li[contains(.,'€')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))
        
        deposit = response.xpath("//li[contains(.,'Guarantee')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€","").strip())

        

        external_id = response.xpath("//span[@class='selectionLink ']/parent::div/ul/li[1]/text()").get()
        if external_id:
            external_id = external_id.split('.')[1].strip()
        item_loader.add_value("external_id", external_id)

        images = [x for x in response.xpath("//div[@class='show-carousel owl-carousel owl-theme']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//div[@class='services details']/ul/li[.='Furnished']").get()
        if furnished:
            furnished = True
        item_loader.add_value("furnished", furnished)

        elevator = response.xpath("//li[.='Lift']/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        washing_machine = response.xpath("//li[.='Washing machine']/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        floor = response.xpath("//li[contains(.,'Floor')]/span/text()").get()
        if floor:
            floor = floor.split('/')[0].strip().lower()
        item_loader.add_value("floor", floor)

        terrace = response.xpath("//div[@class='areas details']/ul/li[contains(.,'Terrace')]/text()").get()
        if terrace:
            terrace = True
        item_loader.add_value("terrace", terrace)

        bathroom = response.xpath("//div//li[contains(.,'bathroom')]//text()").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("bathroom")[0])

        swimming_pool = response.xpath("//div[@class='services details']/ul/li[contains(.,'Swimming pool')]/text()").get()
        if swimming_pool:
            swimming_pool = True
        item_loader.add_value("swimming_pool", swimming_pool)

        washing_machine = response.xpath("//div[@class='services details']/ul/li[contains(.,'Washing machine')]/text()").get()
        if washing_machine:
            washing_machine = True
        item_loader.add_value("washing_machine", washing_machine)

        landlord_name = response.xpath("//div[@class='agency']/h2/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
        item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//span[@class='mail smallIcon']/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
            
        item_loader.add_value("landlord_phone", "+33 4 93 47 16 16")
        utilities = response.xpath("//div//li[contains(.,'Fees')]/span//text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        energy = response.xpath("//div[h2[contains(.,'Energy')]]/img[1]/@src").extract_first()
        if energy:
            energy_label = energy.split("/")[-1]
            if "%" in energy_label:
                energy_label = energy_label.split("%")[0]
            item_loader.add_value("energy_label",energy_label_calculate(energy_label.strip()))
        status = response.xpath("//div//li[contains(.,'Availability')]/span//text()[.!='Rented']").extract_first()
        if status:
            yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label