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
    name = 'amsterdambeautiful_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    start_urls = ["https://www.amsterdambeautiful.nl/en/doorzoek-woningen/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='woning-buitenkant']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[contains(@class,'eerste-beschrijving-container')]//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        item_loader.add_value("external_source", "Amsterdambeautiful_PySpider_netherlands")

        address = response.xpath("//strong[contains(.,'Overview:')]/following-sibling::text()[contains(.,'Address')]").get()
        if address:
            item_loader.add_value("address", address.split(':')[-1].strip())
            item_loader.add_value("zipcode", " ".join(address.split(':')[-1].strip().split(' ')[-2:]).strip())
            item_loader.add_value("city", " ".join(address.split(':')[-1].strip().split(' ')[:-2]).strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'eerste-beschrijving-container')]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//span[@class='icoon-beschrijving' and contains(.,'Surface')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].split('.')[0].strip())

        room_count = response.xpath("//span[@class='icoon-beschrijving' and contains(.,'Bedrooms')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//strong[contains(.,'Overview:')]/following-sibling::text()[contains(.,'Toilets')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("Toilets")[1].strip().replace(":","")
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//span[@class='icoon-beschrijving' and contains(.,'Price')]/following-sibling::span/text()").get()
        if rent:
            rent = rent.split('€')[-1].split('/')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = "".join(response.xpath("//span[@class='icoon-beschrijving' and contains(.,'Available')]/following-sibling::span//text()").getall()).strip()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('from')[-1].strip().replace('immediately', 'now'), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                if date_parsed.year < today.year:
                    return
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='your-class']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//div[contains(@id,'maps-render')]/div/@data-markerlat").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())

        longitude = response.xpath("//div[contains(@id,'maps-render')]/div/@data-markerlon").get()
        if longitude:
            item_loader.add_value("longitude", longitude.strip())
        
        energy_label = response.xpath("//strong[contains(.,'Overview:')]/following-sibling::text()[contains(.,'Energy rating')]").get()
        if energy_label:
            if energy_label.split(':')[-1].strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.split(':')[-1].strip().upper())
        
        parking = response.xpath("//strong[contains(.,'Overview:')]/following-sibling::text()[contains(.,'Parking')]").get()
        if parking:
            if 'permit' in parking.strip().lower():
                item_loader.add_value("parking", True)
            elif 'none' in parking.strip().lower():
                item_loader.add_value("parking", False)

        furnished = response.xpath("//span[@class='icoon-beschrijving' and contains(.,'Interior')]/following-sibling::span/text()").get()
        if furnished:
            if furnished.strip().lower() == 'furnished':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'unfurnished':
                item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_name", "Amsterdam Beautiful Property Rental")
        item_loader.add_value("landlord_phone", "+31 (0)20 330 7338")
        item_loader.add_value("landlord_email", "info@amsterdambeautiful.nl")
              
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None