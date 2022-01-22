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
from datetime import datetime
import dateparser


class MySpider(Spider):
    name = 'relowonen_nl_disabled'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.relowonen.nl/",
                    
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//div[@class='listing__item']//div[@class='properties__info']/a"):
            following = url.xpath("./@href").extract_first()
            yield Request(following,callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Relowonen_PySpider_netherlands")

        rented = response.xpath("//div[contains(@class,'property__ribon')]//text()[contains(.,'rented')]").extract_first()
        if rented:return
        item_loader.add_value("external_link", response.url)
        # item_loader.add_value("latitude", response.meta["lat"])
        # item_loader.add_value("longitude", response.meta["lng"])
        address = response.xpath("//ul[@class='property__params-list']/li[contains(.,'Adres')]/strong/text()").get()
        if address:
            item_loader.add_value("address", str(address))
            if "Hoorn" in address:
                item_loader.add_value("landlord_phone", "0229-760012")
            elif "Purmerend" in address:
                item_loader.add_value("landlord_phone", "0299-760023")
            elif "Zaandam" in address:
                item_loader.add_value("landlord_phone", "075-7600013")
        else:
            address = response.xpath("//ul[@class='property__params-list']/li[contains(.,'Addres')]/strong/text()").get()
            if address:
                item_loader.add_value("address", str(address))
                if "Hoorn" in address:
                    item_loader.add_value("landlord_phone", "0229-760012")
                elif "Purmerend" in address:
                    item_loader.add_value("landlord_phone", "0299-760023")
                elif "Zaandam" in address:
                    item_loader.add_value("landlord_phone", "075-7600013")
        item_loader.add_xpath("room_count", "//dl[@class='property__plan-item']/dd[contains(.,'Bedroom')]/following-sibling::dd/text()")
        citycheck=item_loader.get_output_value("address")
        if citycheck:
            city=citycheck.split(",")[-1]
            item_loader.add_value("city",city)
        f_text = "".join(response.xpath("//div[@class='property__info']/div/strong/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'property__description')]//div[contains(@class,'wpb_wrapper')]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//li[contains(.,'Woonoppervlakte')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", "".join(filter(str.isnumeric, square_meters.strip().replace('m2', ''))))
        
        bathroom_count = response.xpath("//dd[contains(.,'Bathroom')]/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[@class='property__price']/strong/text()").get()
        if rent:
            rent = rent.split('€')[-1].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/strong/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        # else:

        # available_date=response.xpath("//ul[@class='property__params-list']/li[contains(.,'Available: ')]/strong/text()[not(contains(.,'direct') or contains(.,'Direct'))][not(contains(.,'direct') or contains(.,'Direct'))]").get()
        # print("----------",available_date)
        # if available_date:
        #     date2 =  available_date.strip()
        #     date_parsed = dateparser.parse(
        #         date2, date_formats=["%m-%d-%Y"]
        #     )
        #     date3 = date_parsed.strftime("%Y-%m-%d")
        #     item_loader.add_value("available_date", date3)

        
        deposit = response.xpath("//li[contains(.,'Deposit')]/strong/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].split(',')[0].split('.')[0].replace("-","").strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='slider__wrap']//img/@data-lazy").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@class='slider__wrap']//img[contains(@data-lazy,'Plattegrond')]/@data-lazy").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//li[contains(.,'energielabel')]/strong/text()").get()
        if energy_label:
            if energy_label.strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.strip().upper())

        utilities = response.xpath("//li[contains(.,'servicecosts')]/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split(',')[0].split('.')[0].replace("-","").strip())

        pets_allowed = response.xpath("//li[contains(.,'Huisdieren niet toegestaan')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        elif response.xpath("//li[contains(.,'Huisdieren toegestaan in overleg')]").get(): item_loader.add_value("pets_allowed", True)
        
        parking = response.xpath("//li[contains(.,'Parkeren') or contains(.,'parkeren')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//ul[@class='property__params-list property__params-list--options']/li[contains(.,'Parking')]/text()").get()
            if parking:
                item_loader.add_value("parking", True)            

        balcony = response.xpath("//li[contains(.,'Balkon') or contains(.,'balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Gemeubileerd') or contains(.,'Gestoffeerd')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        if response.xpath("//li[contains(.,'Afwasmachine') or contains(.,'afwasmachine')]").get(): item_loader.add_value("dishwasher", True)
        elif response.xpath("//li[contains(.,'Wasmachine') or contains(.,'wasmachine')]").get(): item_loader.add_value("washing_machine", True)

        item_loader.add_xpath("landlord_name", "//h3[@class='worker__name fn']/a/text()")
        item_loader.add_xpath("landlord_email", "//dl[@class='email']/dd/a/text()")
        

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("room" in p_type_string.lower()):
        return "room"
    else:
        return None
              
   