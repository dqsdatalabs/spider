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
    name = 'immodefrance_cotedazur_com'    
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Immodefrance_Cotedazur_PySpider_france"

    custom_settings = {
        "PROXY_ON":True,
    }

    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"1", "house":"2"}

        for key, value in kwargs.items():
            formdata = {
                "location_search[commune]": "",
                "location_search[typeBien][]": value,
                "location_search[loyer_min]": "0",
                "location_search[loyer_max]": "1000000",
                "location_search[surface_min]": "",
                "location_search[noMandat]": "",
                "location_search[tri]": "loyerCcTtcMensuel|asc",
            }
            yield FormRequest("https://www.immodefrance-alpesmaritimes.com/fr/locations",
                            callback=self.parse,
                            formdata=formdata,
                            meta={'property_type': key})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 20)

        seen = False
        for item in response.xpath("//div[contains(@class,'article')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 20 or seen:
            p_url = f"https://www.immodefrance-alpesmaritimes.com/fr/map/mini-fiche/Location/{page}/normal/loyerCcTtcMensuel%7Casc"
            yield Request(
                p_url,
                dont_filter=True,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+20}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        rent="".join(response.xpath("//div[@class='prix_resp']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        square_meters=response.xpath("//div[@class='criteres']/div[contains(.,'habitable')]/text()").get()
        if "." in square_meters:
            square_mt=square_meters.split(":")[1].split("m²")[0].strip()
            item_loader.add_value("square_meters", str(round(float(square_mt))))
        elif square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].split("m²")[0].strip())
        bathroom_count=response.xpath("//div[@class='criteres']/div[contains(.,'salle de bain') or contains(.,'Salle d') ]/text()").get()

        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1])

        room_count=response.xpath("//div/h2[@class='clr-location']//text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" pièce")[0].split(" ")[-1])
        else:
            room_count=response.xpath("//p[@class='chambre']/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0].strip())
        
        attr="".join(response.xpath("//div[@class='titre']/h3/text()").getall())
        if attr:
            address=attr.split("|")[0].replace('(','').replace(')','')
            item_loader.add_value("address", address.strip())
            try:
                city = attr.split("|")[0].split("(")[0].strip()
                zipcode = attr.split("|")[0].split("(")[1].split(")")[0].strip()
                if "Lyon" in city:
                    city = city.split(" ")[0].strip()
                item_loader.add_value("city", city.strip())
                item_loader.add_value("zipcode", zipcode.strip())
            except:
                pass
            
            ref=attr.split('|')[1].split(':')[1]
            item_loader.add_value("external_id", ref.strip())

        desc="".join(response.xpath("//div[@class='descriptif']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "meublé" in desc:
                item_loader.add_value("furnished", True)

        images=[x for x in response.xpath("//div[@id='carousel-photo']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        floor=response.xpath("//div[@class='criteres']/div[contains(.,'Étage')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
            
        utilities=response.xpath("//div[@class='criteres']/div[contains(.,'dont état des lieux')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip().replace(" ",""))
        
        deposit=response.xpath("//div[@class='criteres']/div[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].strip().replace(" ",""))
        
        energy_label=response.xpath("//table[contains(@class,'tableau_conso')]/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("dpe_")[1])
            
        parking=response.xpath("//p[@class='parking']/text()").get()
        garage=response.xpath("//div[@class='criteres']/div[contains(.,'garage')]/text()").get()
        if parking or garage:
            item_loader.add_value("parking",True)

        
        latlng = response.xpath("//script[contains(.,'latLng')]/text()").get()
        if latlng:
            lat = latlng.split("position = ")[1].split(",")[0].strip().replace("[","")
            lng = latlng.split("position = ")[1].split(",")[1].strip().split("]")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
            
        landlord_phone = response.xpath("//span//@data-tel").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        yield item_loader.load_item()

