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
    name = 'agda_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    # custom_settings = {
    #     "PROXY_ON" : True
    # }

    def start_requests(self):
        url =  "https://www.agda.fr/search/location?page=1&bounds=44.35527821160296,4.575805664062501,46.897739085507,6.3885498046875&tri=dateAjout-desc&json=1"
        yield Request(url,
            callback=self.parse,dont_filter=True
            )
        
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False

        data = json.loads(response.body)["listing"]
        sel = Selector(text=data, type="html") 
        for item in sel.xpath("//div[@class='housing-list-tile row bien ']/a/@href").extract():
            yield Request(f"{item}", callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            f_url = f"https://www.agda.fr/search/location?page={page}&bounds=44.35527821160296,4.575805664062501,46.897739085507,6.3885498046875&tri=dateAjout-desc&json=1"
            yield Request(f_url, callback=self.parse, meta={"page": page+1})
    def populate_item(self, response): 

        item_loader = ListingLoader(response=response)
        
        property_type = ""
        prop = response.xpath("//ul//span[@itemprop='name']/text()[contains(.,'Maison')]").extract_first()
        if prop:
            property_type = "house"
        elif response.xpath("//ul//span[@itemprop='name']/text()[contains(.,'Appartements')]").extract_first():
            property_type = "apartment"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", "Agda_PySpider_france")
        # item_loader.add_value("latitude", response.meta.get("latitude"))
        # item_loader.add_value("longitude", response.meta.get("longitude"))
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("external_id", "substring-after(//p[@class='m-no small']/i/text(),': ')")

        rent = " ".join(response.xpath("//div[contains(@class,'prix')]/div[contains(@class,'h3')]/text()").getall())  
        if rent:
            price = rent.split("/")[0].replace("\xa0","").replace(" ","").strip()
            item_loader.add_value("rent_string", price)

        address = " ".join(response.xpath("//div[@itemprop='address']/div//text()").getall())  
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_xpath("city", "//div[@itemprop='address']/div[@itemprop='addressLocality']/text()")

        meters = " ".join(response.xpath("//h1//span[@itemprop='name']//text()[contains(.,'m²')]").getall())  
        if meters:
            s_meters = meters.split("m²")[0].strip().replace(",",".").split(" ")[-1].strip()
            item_loader.add_value("square_meters", int(float(s_meters))) 

        zipcode = " ".join(response.xpath("//h1//span[@itemprop='name']//text()").getall())  
        if zipcode:
            city = "".join(item_loader.get_collected_values("city"))
            item_loader.add_value("zipcode",zipcode.split(city)[0].split(",")[-1].strip())

        room_count = " ".join(response.xpath("//div[@class='caracteristique ellipsis']/text()[contains(.,'Chambre')]").getall())  
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0].strip())
        else:
            room2 = " ".join(response.xpath("//div[@class='caracteristique ellipsis']//text()[contains(.,'Cave')]").getall())
            studio = " ".join(response.xpath("//h1//span[@itemprop='name']//text()").getall())  
            if "studio" in studio.lower():
                item_loader.add_value("room_count", "1")
            elif room2:
                item_loader.add_value("room_count",room2.strip().split(" ")[0].strip())
            else:
                item_loader.add_xpath("room_count","substring-after(substring-before(//h1//span[@itemprop='name']//text(),'pièce'),'louer ')")

        bathroom_count = " ".join(response.xpath("//div[@class='caracteristique ellipsis']/text()[contains(.,'Salle d')]").getall())  
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0].strip() )

        floor = " ".join(response.xpath("//div[@class='caracteristique ellipsis']/span[@class='icon font-agda-etage']/following-sibling::text()").getall())  
        if floor:
            floor = floor.split("Étage")[1].strip()
            item_loader.add_value("floor",floor.strip() )

        description = " ".join(response.xpath("//div[@class='description']/div[2]/text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//div[@class='dpe']/text(),': '),'(')")

        utilities = "".join(response.xpath("substring-after(//ul/li//text()[contains(.,'Charges')],':')").extract())
        if utilities:
            uti = utilities.split("/")[0].strip().replace("\xa0","")
            item_loader.add_value("utilities", uti.strip())

        images = [x.split("(")[1].split(")")[0].strip() for x in response.xpath("//div[@class='carousel-slides']/div/@style[not(contains(.,'background-image:none'))]").getall()]
        if images:
            item_loader.add_value("images", images)

        deposit = "".join(response.xpath("substring-after(//ul/li//text()[contains(.,'Dépôt de garantie')],':')").extract())
        if deposit: item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.split(".")[0].strip())))

        available_date=response.xpath("//div[@class='label-liberation orange h4']/text()").get()
        if available_date:
            date2 =  available_date.strip().split(" ")[-1].replace("immédiatement","now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
        
        parking = "".join(response.xpath("//span[contains(@class,'garage')]//parent::div/text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//div[@class='caracteristique ellipsis']/text()[contains(.,'Terrasse')]").getall())
        if terrace:
            item_loader.add_value("terrace",True)

        balcony = "".join(response.xpath("//div[@class='caracteristique ellipsis']/text()[contains(.,'Balcon')]").getall())
        if balcony:
            item_loader.add_value("balcony",True)

        elevator = "".join(response.xpath("//div[@class='caracteristique ellipsis']/text()[contains(.,'Ascenseur')]").getall())
        if elevator:
            item_loader.add_value("elevator",True)
 
        furnished = "".join(response.xpath("//div[@class='ribbon-left-side']//text()[contains(.,'Meublé')]").getall())
        if furnished:
            item_loader.add_value("furnished",True)

        item_loader.add_xpath("landlord_phone", "normalize-space(//div[@class='numero-telephone']/text())")
        item_loader.add_xpath("landlord_name", "//h3[@class='nom h4 m-no']/text()")
        landlord_email = response.xpath("//div[contains(@class,'email')]//script//text()").get()
        if landlord_email:
            name = landlord_email.split("write('")[1].split("'")[0]
            mail = landlord_email.split("'@'+'")[1].split("'")[0]
            item_loader.add_value("landlord_email", name + '@' + mail)

        url=response.xpath("//div[@class='row conseiller-row p-small-bottom p-small-top']//@src").get()
        if url:
            url=f"https://www.agda.fr{url}"
            yield Request(url, callback=self.landlord, meta={"item_loader":item_loader})

    def landlord(self,response):
        item_loader=response.meta.get("item_loader")
        name=response.xpath("//h3[@class='nom h4 m-no']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='numero-telephone']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)


        yield item_loader.load_item()