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
    name = 'henroimmo_be'
    execution_type='testing'
    country='belgium'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"typeS": "Appartement/Loft", "property_type": "apartment"},
            {"typeS": "Maison", "property_type": "house"},
	        {"typeS": "Villa", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for t in start_urls:
            headers = {
                "content-type": "application/x-www-form-urlencoded",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
                "origin": "https://www.henro-immo.be"
            }

            data = {
                "statut": "L",
                "localiteS": "",
                "prixmaxS": "",
                "refS": "",
                "typeS": t['typeS'],
                "chambreS": "",
                "facadeS": "",
                "noVendu": "1",
            }

            url = "https://www.henro-immo.be/Chercher-bien-accueil--L--resultat"        
            yield FormRequest(
                url,
                formdata=data,
                headers=headers,
                dont_filter=True,
                callback=self.parse,
                meta={"property_type":t["property_type"],
                    "typeS":t["typeS"]
                },
            )

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@class='container']//div[contains(@class,'col-lg-4')]/div['product-grid']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 1 or seen:
            pagination = "https://www.henro-immo.be/Chercher-bien-accueil--L--resultat?pagin=page_count&statut=L&localiteS=&typeS=prop_type&prixmaxS=&chambreS=&piscineS=&garageS=&noVendu=1&keyword=&"
            typeS = response.meta.get("typeS")
            url = pagination.replace(f"page_count",f"{str(page)}").replace("prop_type", typeS)
            yield Request(url, callback=self.parse, meta={"page": page+1, "typeS":typeS, "property_type":property_type, "pagination":pagination})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Henro_Immo_PySpider_belgium")
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1/text()")

        external_id = "".join(response.xpath("//ul[@class='padding-0']/li[contains(.,'REF')]/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split("REF")[1].strip())

        rent = "".join(response.xpath("//div[@class='col-md-6 col-lg-12']/div[contains(.,'€')]/text()").extract())
        if rent:
            price =  rent.replace(" ","")
            item_loader.add_value("rent_string", price)
        else:
            price = "".join(response.xpath("//div[contains(@class,'desc')]/text()").extract())
            if price and "Prix" in price:
                price = price.split("Prix")[1].split("€")[0].replace(":","").strip()
                item_loader.add_value("rent", int(float(price))*4)
            item_loader.add_value("currency", "EUR")


        meters = "".join(response.xpath("//ul[@class='padding-0']/li[contains(.,'Surface hab')]/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split(":")[1].split("m")[0].strip())

        room = "".join(response.xpath("//ul[@class='padding-0']/li[contains(.,'Chambre(s)')]/text()").extract())
        if room:
            item_loader.add_value("room_count", room.split(":")[1].strip())
  
        bathroom_count = response.xpath("//ul[@class='padding-0']/li[contains(.,'Salle de douche') or contains(.,'Salle de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())

        address = "".join(response.xpath("//ul[@class='padding-0']/li[contains(.,'Région')]/text()").extract())
        if address:
            item_loader.add_value("address", address.split(":")[1].strip())
            item_loader.add_value("city", address.split(":")[1].strip())
        else:
            address = "".join(response.xpath("//ul[@class='padding-0']/li[contains(.,'Localité')]/text()").extract())
            if address:
                item_loader.add_value("address", address.split(":")[1].strip())
                item_loader.add_value("city", address.split(":")[1].strip())

        energy_label = "".join(response.xpath("substring-before(substring-after(//div/img/@src[contains(.,'peb')],'peb/'),'.')").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())

        desc = "".join(response.xpath("//div[contains(@class,'desc')]/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='team-image']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        parking = "".join(response.xpath("//ul[@class='padding-0']/li[contains(.,'Garage')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        phone = " ".join(response.xpath("//div[@class='col-md-6 col-lg-12']/div/div/div/text()").getall()).strip()   
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        name = " ".join(response.xpath("//div[@class='col-md-6 col-lg-12']/div/div[2]/text()[1]").getall()).strip()   
        if name:
            item_loader.add_value("landlord_name", name.strip())
        item_loader.add_value("landlord_email", "info@henro-immo.be")
        

        yield item_loader.load_item()