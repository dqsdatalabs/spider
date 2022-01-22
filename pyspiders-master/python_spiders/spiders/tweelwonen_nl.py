# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import math
import re

class MySpider(Spider):
    name = 'tweelwonen_nl'
    start_urls = ['https://www.tweelwonen.nl/woningaanbod/huur?iscustom=true'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    # LEVEL 1

    def start_requests(self):
        start_urls = [
            # {
            #     "url" : "https://www.tweelwonen.nl/woningaanbod/huur/type-appartement?iscustom=true&orderby=8&orderdescending=true",
            #     "property_type" : "apartment"
            # },
            {
                "url" : "https://www.tweelwonen.nl/woningaanbod/huur/type-appartement,woonhuis?iscustom=true&orderby=3",
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 )



    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//article[contains(@class,'objectcontainer')]//a[contains(@class,'property-link')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
               
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Tweelwonen_PySpider_" + self.country + "_" + self.locale)
        
        title = response.xpath('/html/head/title/text()').get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("//tr[./td[.='Referentienummer']]/td[2]/text()").get())

        desc = "".join(response.xpath("//div[@class='description textblock']/div/text()").extract()).strip().replace('_', '')
        if desc:
            item_loader.add_value("description", desc.strip())
        if "zwembad" in desc:
            item_loader.add_value("swimming_pool", True)
        if " wasmachine" in desc:
            item_loader.add_value("washing_machine", True)
        if "vaatwasmachine" in desc:
            item_loader.add_value("dishwasher", True)
        if "huisdieren is in overleg toegestaan" in desc:
            item_loader.add_value("pets_allowed", True)
            
        latLng = response.xpath("//script[contains(.,'object_detail_google_map')]").get()
        if latLng:
            latitude = (latLng.split("center: ")[1]).split("]")[0].strip("[").split(",")[1].strip()
            longitude = (latLng.split("center: ")[1]).split("]")[0].strip("[").split(",")[0].strip()
            if latitude and longitude:
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
        
        address = response.xpath("//div[contains(@class,'object_details')]/h2/text()").extract_first()
        if address:
            address = address.split(":")[1].strip()
            item_loader.add_value("address", address)
            address = address.split(",")[-1].strip()

            zipcode =" ".join(address.split(" ")[0:2])
            if any(c for c in zipcode if c.isdigit()):
                item_loader.add_value("zipcode", zipcode.strip())
            
            city = address.replace(zipcode,"")
            if city:
                item_loader.add_value("city", city.strip())

        prop = " ".join(response.xpath("//table[@class='table table-condensed object_table_details']//tr[td[.='Type object']]/td[2]/text()").extract())
        if get_p_type_string(prop):
            item_loader.add_value("property_type", get_p_type_string(prop))
        else: return
        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        square_meters = response.xpath("//tr[./td[.='Gebruiksoppervlakte wonen']]/td[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            if "," in square_meters:
                square_meters = square_meters.replace(",",".")
                square_meters = math.ceil(float(square_meters))
            item_loader.add_value("square_meters", str(square_meters))
        

        #room_count = response.xpath("substring-after(//tr[./td[.='Aantal kamers']]/td[2]/text(),'(')").get()
        room_count = re.search(r'Aantal kamers(\d)', ''.join(response.xpath("//div[@id='tabs-2']//tr//td/text()").extract()))
        if room_count:
            #room_count = room_count.strip().split(" ")[1].split("slaapkamers")[0].strip()
            item_loader.add_value("room_count", room_count.group(1))
        
        bathroom_count=response.xpath("//tr[./td[.='Aantal badkamers']]/td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        available_date = response.xpath("//tr[./td[.='Aanvaarding']]/td[2]/text()").get()
        if available_date and available_date.replace(" ","").isalpha() != True:
            try:
                available_date_list = available_date.split(" ")
                available_date = available_date_list[2] + " " + available_date_list[3] + " " + available_date_list[4]
            except:
                pass
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        

        images = [x for x in response.xpath("//div[@id='object-photos']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        

        price = response.xpath('//div[@class="object_price"]/text()').get()
        if price:
            price = price.split(",")[0].strip("€").strip()
            item_loader.add_value("rent", price)

        item_loader.add_value("currency", "EUR")
        


        deposit = response.xpath("//tr[./td[.='Borg']]/td[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(",")[0].strip("€").strip())
        
        

        furnished = response.xpath("//tr[./td[.='Inrichting']]/td[2]/text()").get()
        if furnished:
            if furnished.lower() == "ja":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        
        

        elevator = response.xpath("//tr[./td[contains(.,'lift')]]/td[2]/text()").get()
        if elevator:
            if elevator.lower() == "ja":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        

        balcony = response.xpath("//tr[./td[contains(.,'balkon')]]/td[2]/text()").get()
        if balcony:
            if balcony.lower() == "ja":
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        

        energy_label = response.xpath("//tr[./td[contains(.,'Energie')]]/td[2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        
        floor = response.xpath("//tr[./td[contains(.,'Aantal bouwlagen')]]/td[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        
        landlord_name = response.xpath("//div[@class='object_detail_contact_name']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[@class='object_detail_department_phone']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        landlord_email = response.xpath('//a[@class="object_detail_department_email obfuscated-mail-link"]/text()').get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        status = response.xpath("//span[@class='object_status rented']/span/text()").get()
        if status and "Verhuurd" in status:
            return
        else:
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    print("-----------",p_type_string)
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif "Appartement" in p_type_string:
        return "apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower() or "conversion" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()or "woonhuis" in p_type_string.lower()):
        return "house"
    else:
        return None
