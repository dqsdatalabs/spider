# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re


class MySpider(Spider):
    name = 'pri_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Pri_PySpider_netherlands"

    # 1. FOLLOWING
    def start_requests(self): 
        formdata = { 
            "sorteer": "Asc~Prijs",
            "prijs": "1000,999999999",
            "prefilter": "Huuraanbod",
            "pagenum": "0",
            "pagerows": "12",
        }
        yield FormRequest(
            "https://www.pri.nl/huizen/smartselect.aspx",
            callback=self.jump,
            formdata=formdata,
            dont_filter=True,)
    
    def jump(self, response):
        data = json.loads(response.body)
        id_list = ""
        for i in data["AllMatches"]:
            id_list = id_list + i + ","
        
        formdata = {
            "id" : id_list.strip(",").strip(),
        }
        yield FormRequest(
            "https://www.pri.nl/huizen/smartelement.aspx",
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,)
    

    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False
        for item in response.xpath("//a[contains(@class,'adreslink')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            formdata = {
                "sorteer": "Asc~Prijs",
                "prijs": "1000,999999999",
                "prefilter": "Huuraanbod",
                "pagenum": str(page),
                "pagerows": "12",
            }
            yield FormRequest(
                "https://www.pri.nl/huizen/smartselect.aspx",
                callback=self.jump,
                formdata=formdata,
                #dont_filter=True,
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[.='Status']/following-sibling::div/text()").get()
        if status and ("onder" in status.lower() or "verhuurd" in status.lower() or "rented" in status.lower()):
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//div[@class='object-detail-extra-text']/text()").get()
        if dontallow and "under option" in dontallow.lower():
            return 


        f_text = " ".join(response.xpath("//div[contains(.,'Type of')]/following-sibling::div/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        external_id = response.url.split('-')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        title = " ".join(response.xpath("//h1/span/text()").getall())
        if title:
            item_loader.add_value("title",title.strip())
        rent = "".join(response.xpath("substring-before(substring-after(//div[@class='detail_prijs prijs_aktief']/h2/text(),'excluding'),'per')").getall())
        if rent:
            price = rent.replace(".","").strip()
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR") 

        deposit = "".join(response.xpath("//div[div[.='Deposit']]/div[2]/text()").getall())
        if deposit:
            item_loader.add_value("deposit",deposit.strip())

        available_date=response.xpath("//div[div[.='Acceptance date']]/div[2]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        description = " ".join(response.xpath("//div[@id='object-description']/div/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())


        room_count = "".join(response.xpath("//div[div[.='Number of bedrooms']]/div[2]/text()").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.strip())  

        address = "".join(response.xpath("//div[@class='object-adres']//text()").getall()) 
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address.strip()))

        item_loader.add_xpath("city","//span[@class='plaatsnaam']/text()")
        item_loader.add_xpath("zipcode","//span[@class='postcode']/text()")

        bathroom_count = "".join(response.xpath("//div[div[.='Number of bathrooms']]/div[2]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())      

        energy_label = "".join(response.xpath("//span[@class='energielabelKlasseD']/text()").getall())
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(" ")[1].strip())   

        meters = "".join(response.xpath("//div[div[.='Usage area Living']]/div[2]/text()").getall())
        if meters:
            item_loader.add_value("square_meters",meters.split("m²")[0].strip())

        elevator = "".join(response.xpath("//div[div[.='Facilities']]/div[2]/text()").extract())
        if elevator:
            if "elevator" in elevator.lower():
                item_loader.add_value("elevator",True)

        parking = "".join(response.xpath("//div[div[.='Type of parking']]/div[2]/text()").extract())
        if parking:
            if "parking" in parking.lower():
                item_loader.add_value("parking",True)

        washing_machine = "".join(response.xpath("//div[div[.='Facilities bathroom']]/div[2]/text()").extract())
        if washing_machine:
            if "wasmachine" in washing_machine.lower():
                item_loader.add_value("washing_machine",True)


        furnished ="".join(response.xpath("//div[div[.='Specific']]/div[2]/text()").extract())
        if furnished:
            if "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)

        try:
            latitude = response.xpath("//input[contains(@id,'mgmMarker')]/@value").get()
            if latitude:
                item_loader.add_value("latitude", latitude.split('~')[2].split(',')[0].strip())
                item_loader.add_value("longitude", latitude.split('~')[2].split(',')[1].strip())
        except:
            pass

        img_url = response.xpath("//section[@class='object-detail-photos']//a/@data-src").extract_first()
        if img_url:
            yield Request(
                img_url,
                callback=self.get_img,
                meta={
                    "item_loader" : item_loader
                }
            )

    def get_img(self, response):
        item_loader = response.meta.get("item_loader")

        images = [x for x in response.xpath("//div[@class='carousel-inner']/div/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        item_loader.add_value("landlord_name", "Prinsen Residence International")
        item_loader.add_value("landlord_phone", "020-4538220")
        item_loader.add_value("landlord_email", "info@pri.nl")
        
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
