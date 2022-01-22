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
    name = 'immotop_lu'
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    post_url = "https://www.immotop.lu/search/"
    current_index = 0
    other_prop = ["100"]
    other_type = ["house"] 
    custom_settings = {
        "PROXY_TR_ON" : True
    }
    def start_requests(self):
        formdata = {
            "f[sort_field]": "ts",
            "f[sort_type]": "desc",
            "f[hidden]": "",
            "f[Type]": "rent",
            "form": "simple_search",
            "sort_field": "ts",
            "sort_type": "desc",
            "f[text_search]": "",
            "f[city]": "",
            "f[Parent_ID][]": "200",
            "f[rooms][from]": "",
            "f[surface][from]": "",
            "f[price][from]": "",
            "f[price][to]": "",
            "f[by_transport]": "",
            "form": "main_search_form",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment", "info":"first"})

     
    total_page = 1 
    # 1. FOLLOWING
    def parse(self, response):
        if response.meta.get("info", "other") == "first":
            self.total_page = int(response.xpath("//h2/span/text()").get().replace(".", ""))
            self.total_page = (self.total_page // 15) + 2

        page = response.meta.get("page", 2)
        for url in response.xpath("//p[@class='title-anons']/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if page <= self.total_page:
            p_url = f"https://www.immotop.lu/search/index{page}.html"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "f[sort_field]": "ts",
                "f[sort_type]": "desc",
                "f[hidden]": "",
                "f[Type]": "rent",
                "form": "simple_search",
                "sort_field": "ts",
                "sort_type": "desc",
                "f[text_search]": "",
                "f[city]": "",
                "f[Parent_ID][]": self.other_prop[self.current_index],
                "f[rooms][from]": "",
                "f[surface][from]": "",
                "f[price][from]": "",
                "f[price][to]": "",
                "f[by_transport]": "",
                "form": "main_search_form",
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': self.other_type[self.current_index], "info":"first"})
            self.current_index += 1
            
 
                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        
        sale = response.xpath("//h1[contains(.,'vendre')]/text()").get()
        if sale:
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Immotop_PySpider_belgium")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        external_id = response.xpath("//span[contains(@class,'ref')]//text()[contains(.,'Réf')]").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)


        if "(fr)" in title.lower():
            return


        address = response.xpath("//div[contains(@class,'city')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//div[contains(@class,'city')]//text()").get()
        if city:
            city = city.split("/")[0].strip()
            item_loader.add_value("city", city)

        square_meters = response.xpath("//div[contains(@class,'sub')][contains(.,'Surface')]//parent::div//div[contains(@class,'title')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].split(".")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//div[contains(@class,'title-block')]//div[contains(@class,'price')]//text()").get()
        price = ""
        if rent:
            price = rent.split("€")[0].strip().replace("\u00a0","").replace("à partir de","").replace("de","").replace("par","")
            if price > "0":
                try:
                    item_loader.add_value("rent", int(float(price)))
                except:
                    price = rent.split("(")[0].strip().replace("\u00a0",".").replace("à partir de","").replace("de","").replace("par","")
                    item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")
        
        if price and int(float(price)) > 99999:
            return

        utilities = response.xpath("//div[contains(@class,'charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].split(",")[0].strip()
            item_loader.add_value("utilities", utilities)

        deposit = response.xpath("//div[contains(@class,'charges')]//text()[contains(.,'Caution')]").get()
        if deposit:
            deposit = deposit.split(",")[0].replace(" ","").strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'description-ctn')]//div[contains(@class,'text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if not desc:
            return

        room_count = response.xpath("//div[contains(@class,'sub')][contains(.,'Chambre')]//parent::div//div[contains(@class,'title')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'sub')][contains(.,'Salle')]//parent::div//div[contains(@class,'title')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'fotorama')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        balcony = response.xpath("//div[contains(@class,'name')][contains(.,'Extérieur')]//following-sibling::div//text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'name')][contains(.,'Extérieur')]//following-sibling::div//text()[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        parking = response.xpath("//div[contains(@class,'sub')][contains(.,'Garage')]//parent::div//div[contains(@class,'title')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[@class='value'][contains(.,'Parking')]").get()
            if parking:
                item_loader.add_value("parking", True)

        furnished = response.xpath("//div[@class='value'][contains(.,'Meublé') or contains(.,'Meuble') ]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'name')][contains(.,'Confort')]//following-sibling::div//text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        energy_label = response.xpath("//div[contains(@class,'name')][contains(.,'Classe énergétique')]//following-sibling::div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        script_map = response.xpath("//script[contains(.,'xlat =')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("xlat =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("xlon =")[1].split(";")[0].strip())
        landlord_name = response.xpath("//div[@class='company-name']/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)
        else: item_loader.add_value("landlord_name", "IMMOTOP")
        
        if not item_loader.get_collected_values("landlord_name"):
            landlord_name = response.xpath("//div[@class='contact-left']//div[@class='title']/text()").get()
            item_loader.add_value("landlord_name", landlord_name.strip())
        
        if landlord_name and "waucomont" in landlord_name.lower():
            return

        if response.xpath("//div[contains(@class,'company-phone-visible')]//a/text()").get():
            item_loader.add_xpath("landlord_phone", "//div[contains(@class,'company-phone-visible')]//a/text()")
        else: item_loader.add_value("landlord_phone", "+352 26 65 44 56")
        
        available_date = response.xpath("//div[div[.='Disponibilité']]/div[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        yield item_loader.load_item()