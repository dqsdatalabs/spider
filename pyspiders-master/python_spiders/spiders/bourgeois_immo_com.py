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
    name = 'bourgeois_immo_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    
    def start_requests(self):

        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "1"
            },
            {
                "property_type" : "house",
                "type" : "2"
            },
        ]

        for item in start_urls:
            formdata = {
                "nature": "2",
                "type[]": item.get("type"),
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "currency": "EUR",
                "homepage": "",
            }
            yield FormRequest(
                "https://www.bourgeois-immo.com/en/search/",
                callback=self.parse,
                formdata=formdata,
                meta={
                    "property_type":item["property_type"]
                }

            )
       


    # 1. FOLLOWING
    def parse(self, response):
       
        for item in response.xpath("//li[@class='ad']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
                 
        next_page = response.xpath("//li[@class='nextpage']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Bourgeois_Immo_PySpider_france")

        external_id = response.xpath("//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(' ')[-1].strip())

        address = response.xpath("//h1/text()").get()
        if address:
            if "- " in address:
                item_loader.add_value("address", address.split('- ')[-1].strip())
            elif " center " in address:
                item_loader.add_value("address", address.split(' center ')[0].strip().split(" ")[-1])
            elif "," in address:
                if "Le Cannet" in address:
                    address = "Le Cannet"
                else:
                    address = address.split(',')[-1].strip()
                if "room " not in address:
                    item_loader.add_value("address", address)
            elif " flat " in address:
                item_loader.add_value("address", address.split(' flat ')[-1].strip())
        city=response.xpath("//h1/text()").get()
        if city:
            city=city.split("-")[-1].strip()
            item_loader.add_value("city",city)
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//p[@class='comment']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split('.')[0].strip())

        room_count = response.xpath("//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('Bedroom')[0].strip())
        else:
            room_count = response.xpath("//li[contains(.,'Rooms ')]/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split('room')[0].strip())

        bathroom_count = response.xpath("//li[contains(.,'Shower room')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('Shower')[0].strip())
        else:
            bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split('Bathroom')[0].strip())

        rent = response.xpath("//h2[@class='price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = ""
        ad1 = response.xpath("//li[contains(.,'Available at')]/span/text()").get()
        ad2 = response.xpath("//li[contains(.,'Availability')]/span/text()").get()
        if ad1:
            available_date = ad1
        elif ad2:
            available_date = ad2
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//li[contains(.,'Guarantee')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip().replace(',', ''))
        
        images = [response.urljoin(x) for x in response.xpath("//section[@class='showPictures']//div[contains(@class,'resizePicture')]//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        energy_label = response.xpath("//img[contains(@alt,'Conventional')]/@src").get()
        if energy_label:
            energy_label = int(float(energy_label.split('/')[-1].strip().replace('%2C', '.')))
            if energy_label <= 50:
                item_loader.add_value("energy_label", 'A')
            elif energy_label >= 51 and energy_label <= 90:
                item_loader.add_value("energy_label", 'B')
            elif energy_label >= 91 and energy_label <= 150:
                item_loader.add_value("energy_label", 'C')
            elif energy_label >= 151 and energy_label <= 230:
                item_loader.add_value("energy_label", 'D')
            elif energy_label >= 231 and energy_label <= 330:
                item_loader.add_value("energy_label", 'E')
            elif energy_label >= 331 and energy_label <= 450:
                item_loader.add_value("energy_label", 'F')
            elif energy_label >= 451:
                item_loader.add_value("energy_label", 'G')
        
        floor = response.xpath("//li[contains(.,'Floor')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", "".join(filter(str.isnumeric, floor.split('/')[0].strip())))

        utilities = response.xpath("//li[contains(.,'Fees')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip().replace(',', ''))
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        elif response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]").get():
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//li[contains(.,'Swimming pool') or contains(.,'swimming pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        washing_machine = response.xpath("//li[contains(.,'Washing machine') or contains(.,'washing machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        dishwasher = response.xpath("//li[contains(.,'Dishwasher') or contains(.,'dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        if response.xpath("//h2[contains(.,'Contact us')]/following-sibling::h4/text()").get():
            item_loader.add_xpath("landlord_name", "//h2[contains(.,'Contact us')]/following-sibling::h4/text()")
        else: item_loader.add_value("landlord_name", "Bourgeois Immobilier")
        
        if response.xpath("normalize-space(//h2[contains(.,'Contact us')]/following-sibling::p/a[contains(@href,'tel')]/text())").get():
            item_loader.add_xpath("landlord_phone", "normalize-space(//h2[contains(.,'Contact us')]/following-sibling::p/a[contains(@href,'tel')]/text())")
        else: item_loader.add_value("landlord_phone", "+33 4 92 28 23 23")

        yield item_loader.load_item()