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
    name = 'huizenbalie_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://huizenbalie.nl/aanbod-van-woningen/?status=te-huur&location=any&keyword="]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//h3[@class='entry-title']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://huizenbalie.nl/aanbod-van-woningen/page/{page}/?keyword&location=any&status=te-huur"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//span[.='Type']/following-sibling::span/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@class='property-content']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        item_loader.add_value("external_source", "Huizenbalie_PySpider_netherlands")

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-1].strip())
            item_loader.add_value("zipcode", address.split(',')[-2].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='property-content']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//div[@class='property-content']/p[contains(.,'Woonoppervlakte:')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('Woonoppervlakte:')[1].split(',')[0].split('m')[0].strip())

        room_count = response.xpath("//span[contains(text(),'Slaapkamers')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//span[contains(text(),'Badkamers')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//span[contains(@class,'price')]/text()").get()
        if rent:
            if 'per maand' in rent.lower():
                rent = rent.split('€')[-1].lower().split('p')[0].strip().replace('.', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'EUR')
            elif 'per week' in rent.lower():
                rent = rent.split('€')[-1].lower().split('p')[0].strip().replace('.', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//text()[contains(.,'Beschikbaar') or contains(.,'beschikbaar')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().replace('beschikbaar', '').replace('vanaf', '').replace('per', '').replace(':', '').strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//text()[contains(.,'Waarborgsom') and contains(.,'maand')]").get()
        if deposit:
            multiple = "".join(filter(str.isnumeric, deposit.strip()))
            if multiple.isnumeric(): item_loader.add_value("deposit", str(int(multiple) * int(rent)))
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'\"lat\"')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('"lat":')[1].split(',')[0].strip().strip('"').strip())
            item_loader.add_value("longitude", latitude.split('"lang":')[1].split(',')[0].strip().strip('"').strip())

        utilities = response.xpath("//text()[contains(.,'Servicekosten:')]").get()
        if utilities:
            item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.strip())))
        
        parking = response.xpath("//text()[contains(.,'Gratis parkeren') and contains(.,'–')]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//text()[contains(.,'Gemeubileerd') or contains(.,'gemeubileerd') and contains(.,'–')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Huizenbalie")
        item_loader.add_value("landlord_phone", "085-1309111")
        item_loader.add_value("landlord_email", "info@huizenbalie.nl")      
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None