# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'vbtverhuurmakelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "VbtVerhuurmakelaars_PySpider_netherlands"

  
    def start_requests(self):
        url = "https://vbtverhuurmakelaars.nl/api/properties/12/1?search=true" # LEVEL 1
        yield Request(url=url, callback=self.parse)

    def parse(self, response):
        data = json.loads(response.body)
        page_count = data["pageCount"]
        page = response.meta.get('page', 2)
        
        for item in data["houses"]:
            url = "https://vbtverhuurmakelaars.nl"+item["url"]
            f_text = item["attributes"]["type"]["category"]
            status = item["status"]["name"]
            if "rentinuse" in status or "rentreserved" in status:
                continue
            property_type = get_p_type_string(f_text)
            if property_type:
                yield Request(url, callback=self.populate_item, meta={'property_type': property_type,"item":item})
                
        if page <= page_count:   
            url = f"https://vbtverhuurmakelaars.nl/api/properties/12/{page}?search=true"
            yield Request( 
                url,
                callback=self.parse,
                meta={"page": page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title))

        item = response.meta.get("item") 
  
        rent=response.xpath("//div[.='Huurprijs']/following-sibling::div/text()").get()
        if rent:
            rent=rent.split(",")[0].replace("\xa0","").replace("€","")
            if rent:
                item_loader.add_value("rent",rent)

        utilities=response.xpath("//div[@class='housedetails']//div[contains(.,'Servicekosten')][1]//following-sibling::div//text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[1].split(",")[0])

        address=response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address",address)

        city=response.xpath("//div[@class='secondary']/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[-1])
        zipcode=response.xpath("//div[@class='secondary']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[0]+zipcode.split(" ")[1])
        description=" ".join(response.xpath("//div[@class='offerText']/p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        rent=item["prices"]["rental"]["price"]
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        
        images=[x.split("url('")[1].split("')")[0] for x in response.xpath("//div[@class='grid-image']/@style").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters="".join(response.xpath("//div[contains(.,'m²')]/text()[1]").getall())
        if square_meters:
            squ=re.findall("\d+",square_meters)
            if int(squ[0])<10:
                return
            item_loader.add_value("square_meters",squ)
        available_date=response.xpath("//div[.='Aanvaarding']/following-sibling::div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        room_count=item.get("rooms",None)
        
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            return
        longitude=item["coordinate"][0]
        if longitude:
            item_loader.add_value("longitude",longitude)
        latitude=item["coordinate"][1]
        if latitude:
            item_loader.add_value("latitude",latitude)
        energy_label=response.xpath("//div[.='Energielabel']/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip().replace("\n",""))
           

        
        item_loader.add_value("landlord_phone", "088-545 46 00")
        item_loader.add_value("landlord_email", "info@vbtverhuurmakelaars.nl")
        item_loader.add_value("landlord_name", "vb&t Verhuurmakelaars")
        
        
        

        
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None