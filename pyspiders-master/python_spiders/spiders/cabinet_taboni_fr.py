# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'cabinet_taboni_fr'
    execution_type='testing' 
    country='france'
    locale='fr'
    external_source="Cabinet_Taboni_PySpider_france"
    def start_requests(self):
        start_urls = [ 
            {
                "property_type" : "apartment",
                "type" : "1",
            },
        ]
        for item in start_urls:
            formdata = {
                "rech": "/Gerance-Location/Offres-location.html",
                "type": item["type"],
                "id_city": "-1",
                "price": "",
            }
            api_url = "https://www.cabinet-taboni.fr/Gerance-Location/Offres-location.html"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        total_page = response.xpath("//div[@id='pagination']/center/a[last()]/text()").get()
        if total_page:
            total_page = int(total_page.strip())
        else:
            total_page = 1
        for item in response.xpath("//a[@class='lst_immo']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page <= total_page:
            formdata = {
                "id_city": "-1",
                "type": response.meta["type"],
                "price": "",
            }
            api_url = f"https://www.cabinet-taboni.fr/Gerance-Location/Offres-location.html?page={page}"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":response.meta["property_type"],
                    "type":response.meta["type"],
                    "page":page+1,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        # ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cabinet_Taboni_PySpider_france", input_type="VALUE")
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", get_num=True, input_value="//span[@class='price_immo']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//tr/td[contains(.,'Pièce')]/following-sibling::td//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", get_num=True, input_value="//tr/td[contains(.,'Étage')]/following-sibling::td//text()", input_type="F_XPATH")
        
        item_loader.add_value("external_id", response.url.split("=")[-1])
        address = response.xpath("//h1/text()").get()
        if address:
            address = address.strip()
            if ":" in address:
                item_loader.add_value("address", address.split(":")[0].strip())
            elif "studio" in address or "magnifique" in address:
                item_loader.add_value("address", address.split(",")[0].replace("Début","").strip())
            else:
                item_loader.add_value("address", address.strip())
        item_loader.add_value("city", "Nice")
        zipcode=response.xpath("//meta[@name='description']/@content").get()
        if zipcode:
            zipcode=zipcode.split("-")[-1].strip().split(" ")[0]
            zip=re.findall("\d+",zipcode)
            if zip:
                item_loader.add_value("zipcode",zip)
                
        room_count = response.xpath("//div[.='Pièces : ']/following-sibling::div/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//td[contains(.,'Chambres')]//parent::tr//td[@class='a_right']//span//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)


        
        square_meters = response.xpath("//tr/td[contains(.,'Surface ')]/following-sibling::td//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        desc = " ".join(response.xpath("//div[@class='descr_immo']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        if desc and "parkings" in desc.lower():
            return 
            
        # if "salle" in desc:
        #     bathroom_count = desc.split("salle")[0].replace("avec","").strip().split(" ")[-1]
        #     if "une" in bathroom_count:
        #         item_loader.add_value("bathroom_count", "1")
        #     elif bathroom_count.isdigit():
        #         item_loader.add_value("bathroom_count", bathroom_count)
        # else:
        bathroom_count = response.xpath("//td[contains(.,'Surfaces des pièces')]//parent::tr//td[@class='a_right']//span[contains(.,'Salle de bains')]//text()").get()
        if bathroom_count:
            bathroom_count=bathroom_count.split("Salle")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        
        if "euros de charges" in desc:
            utilities = desc.split("euros de charges")[0].strip().split(" ")[-1]
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CABINET TABONI", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 93 88 84 14", input_type="VALUE")

        yield item_loader.load_item()
