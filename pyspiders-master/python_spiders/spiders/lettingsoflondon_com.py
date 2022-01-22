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
from word2number import w2n

class MySpider(Spider):
    name = 'lettingsoflondon_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.lettingsoflondon.com/property-to-rent/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[.='Details']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.lettingsoflondon.com/property-to-rent/?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[@id='home']//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Lettingsoflondon_PySpider_united_kingdom")

        title = " ".join(response.xpath("//div[contains(@class,'detailTitleSpace')]//h2/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//div[contains(@class,'detailTitleSpace')]//h2//a//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            city = address.split(",")[-2]
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.strip().replace("£","").split(".")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Deposit') or contains(.,'deposit')]").get()
        if deposit:
            if "week" in deposit:
                deposit = deposit.split("week")[0].strip()
                if " " in deposit:
                    deposit = deposit.split(" ")[-1]
                rent_week = int(rent)/4
                deposit = int(deposit)*int(rent_week)
            elif "month deposit" in deposit:
                deposit = deposit.split("month deposit")[0].strip().split(" ")[-1]
                if deposit.isdigit():
                    deposit = int(deposit)*int(rent)
                else:
                    try:
                        deposit = w2n.word_to_num(deposit)*int(rent)
                    except :
                        pass
            elif "x Deposit":
                deposit = deposit.split("x")[0].strip()
                deposit = int(deposit)*int(rent)
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[@id='home']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//text()").get()
        if room_count:
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
            else:
                room_count = room_count.strip()
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'carousel slide')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'tab-content')][contains(.,'Available From:')]//text()").getall())
        if available_date:
            available_date = available_date.split("Available From:")[1].strip().split("\t")[0]
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Parking') or contains(.,' parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Balcon') or contains(.,' balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Furnished')]").get()
        if furnished and "unfurnished" not in furnished.lower():
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Elevator')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        energy_label = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'EPC') or contains(.,'epc')]").get()
        if energy_label:
            if "rating" in energy_label:
                energy_label = energy_label.split("rating")[1]
            elif "(" in energy_label:
                energy_label = energy_label.split("(")[1].split(")")[0]
            else:
                energy_label = energy_label.split("EPC")[1].strip()
            item_loader.add_value("energy_label", energy_label)
            
        dishwasher = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Washing machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        pets_allowed = "".join(response.xpath("//div[contains(@class,'tab-content')]//text()[contains(.,'Pets')]").getall())
        if pets_allowed and "not" not in pets_allowed.lower():
            item_loader.add_value("pets_allowed", True)

        latitude_longitude = response.xpath("//div[contains(@id,'map')]//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('q=')[1].split(',')[0]
            longitude = latitude_longitude.split('q=')[1].split(",")[1].split('&')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "LETTINGS OF LONDON")
        item_loader.add_value("landlord_phone", "0203 295 5000")
        item_loader.add_value("landlord_email", "info@lettingsoflondon.com")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif "room" in p_type_string.lower():
        return "room"
    else:
        return None