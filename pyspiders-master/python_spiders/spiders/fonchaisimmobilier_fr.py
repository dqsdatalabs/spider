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
import dateparser


class MySpider(Spider):
    name = 'fonchaisimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ['https://www.fonchais-immobilier.fr/biens-a-louer']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property']"):
            follow_url = response.urljoin(item.xpath("./a/@href").extract_first())
            city = item.xpath("normalize-space(.//div[@class='location']/text())").extract_first()
            prop_type = item.xpath("normalize-space(.//div[@class='type']/div/text())").extract_first()
            if "Appartement" in prop_type:
                prop_type = "apartment"
            elif "Neuf" in prop_type:
                prop_type = "apartment"
            else: prop_type = ""
            
            if prop_type:
                yield Request(follow_url, callback=self.populate_item, meta={"city": city, "property_type": prop_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        city = response.meta.get("city")

        
        item_loader.add_css("title", "div.infos > div.asset")
        item_loader.add_value("external_source", "Fonchaisimmobilier_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
            
        item_loader.add_value("city", city)
        item_loader.add_value("address", city)
   
        square_meters = response.xpath("//div[@class='body']//div[contains(.,'Surface')]/following-sibling::div[1]//text()").extract_first()
        if square_meters:
            if "m" in square_meters :                        
                item_loader.add_value("square_meters", square_meters.split("m")[0])                  

        coordinat = response.xpath("//script/text()[contains(.,'center: [')]").extract_first() 
        if coordinat:
            try:
                map_coor = coordinat.split('center: [')[1].split("],")[0]
                lng = map_coor.split(",")[0].strip()
                lat = map_coor.split(",")[1].strip()
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
            except:
                pass
        price = response.xpath("//div[@class='body']//div[contains(.,'Loyer')]/following-sibling::div[1]//text()").extract_first()
        if price :
            price=price.strip().lstrip("0").replace(" ","")            
            item_loader.add_value("rent_string", price)                  

        room_count = "".join(response.xpath("//div[@class='body']//div[contains(.,'Pièces')]/following-sibling::div[1]//text()").extract())
        if room_count :
            if "Pièces" in room_count:
                item_loader.add_value("room_count", room_count.split("Pièces")[0])
               

        desc = "".join(response.xpath("//div[@class='col-lg-7']//p//text()").extract())
        if desc :
            item_loader.add_value("description", desc.replace("\n"," "))
            deposit=desc.split("locataire : ")[1]
            if deposit:
                item_loader.add_value("deposit", deposit.split("€")[0])
            util = re.search(r"(\d+) €\s*\w* de charges", desc.lower())
            if util:
                item_loader.add_value("utilities", util.group(1))

            available_date = re.search(r"libre le.*(\d{2}.\d{2}.\d{4})", desc.lower())
            if available_date:
                date_parsed = dateparser.parse(available_date.group(1), date_formats=["%m-%d-%Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                if date2:
                    item_loader.add_value("available_date", date2)

        if "studio" in desc.lower(): item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta.get("property_type"))
        
        external_id = response.xpath("//div[@class='body']//div[contains(.,'Référence')]/following-sibling::div[1]//text()").extract()
        if external_id :
            item_loader.add_value("external_id", external_id)


        img_url = response.xpath("//div[contains(@class,'carousel-item')]//@style").extract()
        if img_url : 
            images = []        
            for item in img_url:
                img=item.split("('")[1]
                img_value=img.split("')")[0]
                images.append(img_value)
            if images:
                item_loader.add_value("images", list(set(images)))

        item_loader.add_value("landlord_name", "FONCHAIS IMMOBILIER")
        item_loader.add_value("landlord_phone", "02 85 67 03 53")

        yield item_loader.load_item()