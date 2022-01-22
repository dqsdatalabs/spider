# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import scrapy
import re

class MySpider(Spider):
    name = 'immodefrance_isere_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "PROXY_ON":True,
    }

    def start_requests(self):
        url = "https://www.immodefrance-isere.com/fr/locations"
        start_urls = [
            {
                "formdata" : {
                    'location_search[typeBien][]': '1',
                    'location_search[loyer_min]': '64',
                    'location_search[loyer_max]': '1000000',
                    'location_search[tri]': 'loyerCcTtcMensuel|asc'
                },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'location_search[typeBien][]': '2',
                    'location_search[loyer_min]': '64',
                    'location_search[loyer_max]': '1000000',
                    'location_search[tri]': 'loyerCcTtcMensuel|asc'
                },
                "property_type" : "house"
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], dont_filter=True, callback=self.parse, meta={'property_type': item.get('property_type'), 'from_sq': True})

    def parse(self, response):

        page = response.meta.get("page", 20)
        seen = False
        
        if response.meta["from_sq"]: 
            doc = str(response.body).split(' .html("')[-1].split('")')[0]
            selector = Selector(text=doc, type="html")
            for item in selector.xpath("//div[contains(@class,'article')]/a/@href").getall():
                seen = True
                yield Request(item.replace("\\", "").replace("'", ""), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        else:
            for item in response.xpath("//div[contains(@class,'article')]/a/@href").getall():
                seen = True
                yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 20 or seen:
            follow_url = f"https://www.immodefrance-isere.com/fr/map/mini-fiche/Location/{page}/normal/loyerCcTtcMensuel%7Casc"
            yield Request(follow_url, callback=self.parse, dont_filter=True, meta={"property_type": response.meta["property_type"], "page": page + 20, "from_sq": False})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Immodefrance_Isere_PySpider_france")

        external_id = response.xpath("//div[contains(@class,'titre')]//h3//text()[contains(.,'Réf. :')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Réf. :")[1].split("|")[0].strip())
        idcheck=item_loader.get_output_value("external_id")
        if not idcheck:
            id=response.xpath("//div[contains(@class,'titre')]//h3//text()[contains(.,'N°')]").get()
            if id:
                item_loader.add_value("external_id",id.split("N°")[1].split("|")[0].strip())
        lat=response.xpath("//script[contains(.,'L.map')]/text()").get()
        if lat:
            lat=lat.split("var position")[-1].split(";")[0]
       
            item_loader.add_value("latitude",lat.split(",")[0].replace("[","").replace("=","").strip())
            item_loader.add_value("longitude",lat.split(",")[-1].replace("]",""))
     
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            title = title.strip()
            item_loader.add_value("title", title)
            address = title.split("louer")[1].split(")")[0].strip()
            city = address.split("(")[0]
            zipcode = address.split("(")[1]
            item_loader.add_value("address", address.replace("(",""))
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = "".join(response.xpath("//p[contains(@class,'prix')]//span[contains(@class,'prix has_sup')]//text()").getall())
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[contains(@class,'criteres')]//div[contains(.,'Dépôt de garantie')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        floor = response.xpath("//div[contains(@class,'criteres')]//div[contains(.,'Étage')]//text()").get()
        if floor:
            floor = floor.split(":")[1].strip()
            item_loader.add_value("floor", floor)

        room_count = response.xpath("//p[contains(@class,'chambre')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//p[contains(@class,'sdb')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//p[contains(@class,'surface')]//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        desc = " ".join(response.xpath("//div[contains(@class,'descriptif')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        terrace = response.xpath("//div[contains(@class,'criteres')]//div[contains(.,'Terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//div[contains(@class,'criteres')]//div[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[contains(@class,'criteres')]//div[contains(.,'garage')]//text()").get()
            if parking:
                item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//div[contains(@class,'valeur_conso')][contains(.,'*')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.replace("*","").strip())

        images = [x for x in response.xpath("//div[contains(@id,'carousel-photo')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        utilities = response.xpath("//div[contains(text(),'dont état des lieux')]/text()").get()
        if utilities: item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.split(':')[-1].strip())))
        
        if not item_loader.get_collected_values("room_count"):
            room_count = response.xpath("//h1/text()[contains(.,'pièce')]").get()
            if room_count: item_loader.add_value("room_count", room_count.split(' pi')[0].strip().split(' ')[-1].strip())

        item_loader.add_value("landlord_name", "IMMO de FRANCE")
        item_loader.add_value("landlord_phone", "04 74 43 89 48")
        email=response.xpath("//script[contains(.,'email')]/text()").get()
        if email:
            email=email.split("email")[-1].split(",")[0].replace('"',"").replace(":","")
            item_loader.add_value("landlord_email",email)


        yield item_loader.load_item()