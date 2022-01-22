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
    name = 'arobaseimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    post_url = "https://www.arobase-immobilier.fr/fr/recherche"
    current_index = 0
    other_prop = ["Maison de ville|25",]
    other_prop_type = ["house"]

    def start_requests(self):
        formdata = {
            "search-form-63553[search][subtype]": "Appartement|5",
            "search-form-63553[search][category]": "Location|2",
            "search-form-63553[search][room_range][room_min]": "",
            "search-form-63553[search][room_range][room_max]": "",
            "search-form-63553[search][price_range][price_min]": "",
            "search-form-63553[search][price_range][price_max]": "",
            "search-form-63553[search][order]": "",
            "search-form-63553[submit]": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//ul/li[@class='property initial']"):

            follow_url = response.urljoin(item.xpath("./a/@href").get())
            address = item.xpath("./a//h3/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"address":address})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.arobase-immobilier.fr/fr/recherche?page={page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        
        elif self.current_index < len(self.other_prop):
            formdata = {
            "search-form-63553[search][subtype]": self.other_prop[self.current_index],
            "search-form-63553[search][category]": "Location|2",
            "search-form-63553[search][room_range][room_min]": "",
            "search-form-63553[search][room_range][room_max]": "",
            "search-form-63553[search][price_range][price_min]": "",
            "search-form-63553[search][price_range][price_max]": "",
            "search-form-63553[search][order]": "",
            "search-form-63553[submit]": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "arobaseimmobilier_fr_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_xpath("external_id", "//li[contains(.,'Référence ')]/span/text()")
        floor =  response.xpath("//li[contains(.,'Étage ')]/span/text()").extract_first()
        if floor: 
            item_loader.add_value("floor", floor.split(" ")[0])
        rent =  response.xpath("//h2[@class='price']/text()").extract_first()       
        if rent:
            item_loader.add_value("rent_string", rent.replace("\u202f", "").replace(" ",""))
        deposit =  response.xpath("//li[contains(.,'de garantie')]/span/text()").extract_first()       
        if deposit:
            item_loader.add_value("deposit", deposit.replace("\u202f", ""))
        utilities =  response.xpath("//li[contains(.,'Provision sur charges')]/span/text()").extract_first()       
        if utilities:
            item_loader.add_value("utilities", utilities.replace("\u202f", ""))
        desc = " ".join(response.xpath("//p[@id='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        elevator =  response.xpath("//li[contains(.,'Ascenseur')]/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        room_count =  response.xpath("//li[contains(.,'Pièce')]/span/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("p")[0])
        square_meters =  response.xpath("//li[text()='Surface ']/span/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])

        address =response.meta.get('address')
        if address:
            item_loader.add_value("address", address)
            if "Paris" in address:
                item_loader.add_value("city", address.strip().split(" ")[0])
            else:
                item_loader.add_value("city", address.strip())
        images = [x for x in response.xpath("//div[@class='slider']//img/@src | //div[@class='slider']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_phone", "+33 6 13 02 53 43")
        item_loader.add_value("landlord_name", "AROBASE IMMOBILIER")
        item_loader.add_value("landlord_email", "arobase-immobilier@hotmail.fr")
        yield item_loader.load_item()