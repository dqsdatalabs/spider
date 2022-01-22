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
    name = 'relayimmo_be'
    execution_type='testing'
    country='belgium'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.relay-immo.be/Chercher-bien-accueil--L--resultat?pagin=0&localiteS=&type=Appartement&prixmaxS=&chambreS=&keyword=&", "property_type": "apartment"},
	        {"url": "https://www.relay-immo.be/Chercher-bien-accueil--L--resultat?pagin=0&localiteS=&type=Maison&prixmaxS=&chambreS=&keyword=&", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'portfolio-item')]//a[@class='portfolio-link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 1 or seen:
            base_url = response.meta.get("base_url")
            url = base_url.replace(f"pagin=0",f"pagin={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":property_type, "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Relay_Immo_PySpider_belgium")
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h2[contains(@class,'display-5')]/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        rent = "".join(response.xpath("//div[@class='prices-box']/h1/text()").extract())
        if rent:
            item_loader.add_value("rent", rent.replace(" ",""))
        item_loader.add_value("currency", "EUR")

        external_id = "".join(response.xpath("//div[@class='prices-box']/h6/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[1].strip())
            
        room = "".join(response.xpath("//div[@class='price-features']/text()[contains(.,'chambre(s)')]").extract())
        if room:
            item_loader.add_value("room_count", room.split("chambre")[0].strip())
        else:
            room = "".join(response.xpath("//h1/text()").extract())
            if "studio" in room.lower():
                item_loader.add_value("room_count", "1")

        bathroom_count = "".join(response.xpath("//div[@class='price-features']/text()[contains(.,'Salle ')]").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Salle")[0].strip())

        square_meters = "".join(response.xpath("//div[@class='price-features']/text()[contains(.,'Habitation')]").extract())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("Habitation")[1].split("m")[0].strip())

        floor = "".join(response.xpath("//div[@class='price-features']/text()[contains(.,' Etage')]").extract())
        if floor:
            item_loader.add_value("floor", floor.split("Etage")[1].replace("N°","").strip())

        available_date=response.xpath("//div[@class='price-features']/text()[contains(.,'Dispo le ')]").get()

        if available_date:
            date2 =  available_date.split("Dispo le")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        energy_label = "".join(response.xpath("substring-before(substring-after(//div[@class='hidden-xs']/img/@src,'peb/'),'.')").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())

        desc = "".join(response.xpath("//div[@class='col-12 col-lg-8']//text()").extract())
        if desc:
            desc = desc.replace("\xa0","")
            item_loader.add_value("description", re.sub("\s{2,}", " ", desc))
        
        if "\u20acde charges" in desc:
            utilities = desc.split("\u20acde charges")[0].strip().split("+")[-1]
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))
        elif "de provision" in desc:
            utilities= desc.split("de provision")[0].replace("\u20ac","").replace("euros","").strip().split("+")[-1]
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))
        elif "€ de charges" in desc:
            utilities = desc.split("€ de charges")[0].strip().split(" ")[-1].replace(",",".")
            item_loader.add_value("utilities", int(float(utilities)))
        elif " de charges" in desc:
            utilities = desc.split(" de charges")[0].replace("euros","").replace("de provisions","").replace("€","").strip().split(" ")[-1]
            if "," in utilities:
                utilities= utilities.split(",")[0]
            if utilities.isdigit():
                item_loader.add_value("utilities",int(float(utilities)))
        elif "provision de" in desc:
            utilities = desc.split("provision de")[1].split(",")[0].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
                
        address = "".join(response.xpath("//div[@class='section-title']/h2//text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            if  "-" in address:
                item_loader.add_value("city", address.split("-")[1].strip())
            elif " " in address:
                item_loader.add_value("city", address.split(" ")[-1].strip())
  
        images = [x for x in response.xpath("//div[contains(@class,'owl-carousel')]//img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        balcony = "".join(response.xpath("//h1/text()").extract())
        if "balcon" in balcony.lower():
            item_loader.add_value("balcony", True)

        terrace = "".join(response.xpath("//div[@class='price-features']/text()[contains(.,' Terrasse')]").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_phone", "32 (0) 69 22 90 99")
        item_loader.add_value("landlord_name", "Relay Immo") 

        yield item_loader.load_item()