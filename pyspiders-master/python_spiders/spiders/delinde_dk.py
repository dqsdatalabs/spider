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
    name = 'delinde_dk'
    execution_type = 'testing'
    country = 'denmark'
    locale ='da'
    external_source="Delinde_PySpider_denmark"
    start_urls = ['https://admin.delinde.dk/wp-json/de-linde/v1/property_bolig?filter={%22property_type_bolig%22:[51]}&limit=-1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            follow_url = f"https://www.delinde.dk/bolig/{item['slug']}"
            yield Request(follow_url, callback=self.populate_item, meta={"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("external_link", response.url)
        property_type = ""
        desc = "".join(response.xpath("//div[@class='Content_narrowContent__zRNqw']//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//div[@class='Content_narrowContent__zRNqw']/h5/text()").get()
        if title:
            item_loader.add_value("title",title)
        item = response.meta.get('item')
        # item_loader.add_value("external_id",int(item["id"]))
        item_loader.add_value("address",item["blocks"][0]["attrs"]["address"])
        address=str(item["blocks"][0]["attrs"]["address"])
        if address:
            zipcode=address.split(",")[-1]
            zipcode=re.findall("\d+",zipcode)
            city=address.split(",")[0] 
            city=re.findall("\D+",city)
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)
        squaremeter=item["blocks"][0]["attrs"]["options"] 
        if squaremeter:
            for squ in squaremeter:
                if squ["key"]=="Areal":
                    squaremeters=str(squ["value"]).split("m")[0].strip().split("-")[0]
                    item_loader.add_value("square_meters",squaremeters)
        price=response.xpath("//div/label[.='Pris']/following-sibling::span/text()").get()
        if price and not "Kommer" in price:
            price= int(price.split("kr")[0].strip().replace(".","").split('-')[0].strip())
            item_loader.add_value("rent",price)
        item_loader.add_value("currency","DKK")
        deposit=response.xpath("//li//label[.='Depositum' or .='DEPOSITUM']/following-sibling::span/text()").get()
        if deposit:
            if "måneders" in deposit:
                item_loader.add_value("deposit", (int(deposit.split()[0])*price))
            else:
                
                item_loader.add_value("deposit",deposit.split("kr.")[-1].split(",")[0])


        desc = "".join(response.xpath("//div[@class='Content_narrowContent__zRNqw']//p/text()").getall())
        if desc:         
            item_loader.add_value("description", desc)
        gallery=item["blocks"][1]["attrs"]["blocks"][0]["value"]["gallery"]
        for image in gallery:
            imag=image["url"]["large"]
            item_loader.add_value("images", f"https://admin.delinde.dk{imag}")
        room_count =response.xpath("//li//label[.='VÆRELSER']/following-sibling::span/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room=response.xpath("//div[@class='Content_narrowContent__zRNqw']/h5/text()").get()
            if room:
                room=re.findall("\d+",room)
                item_loader.add_value("room_count",room)
        utilities1=response.xpath("//li//label[contains(text(),'Aconto bidrag') or contains(text(),'ACONTO VAND')]/following-sibling::span/text()").get()
        if utilities1:
            item_loader.add_value("utilities",utilities1.split("kr")[0])
        latitude=response.xpath("//script[contains(.,'maps/place')]/text()").get()
        if latitude:
            latitude=latitude.split("benhavn/@")[-1].split(",")[1]
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//script[contains(.,'maps/place')]/text()").get()

        if longitude:
            longitude=longitude.split("benhavn/@")[-1].split(",")[0]
            item_loader.add_value("longitude",longitude)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li//label[.='OVERTAGELSE' or .='Overtagelse']/following-sibling::span/text()").get()
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        datecheck=item_loader.get_output_value("available_date")
        if not datecheck:
            date=response.xpath("//li//label[.='OVERTAGELSE']/following-sibling::span/text()").get()
            if date:
                date=" ".join(date.split(" ")[-2:])
                if date:
                    date_parsed = dateparser.parse(date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        item_loader.add_value("landlord_name","DELINDE")
        item_loader.add_value("landlord_phone", "86 16 57 00")
        item_loader.add_value("landlord_email", "info@delinde.dk")

        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("lejlighed" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and (" hus " in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None