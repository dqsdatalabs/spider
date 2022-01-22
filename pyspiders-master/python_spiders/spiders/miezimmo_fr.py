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
    name = 'miezimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://miezimmo.fr/recherche/?transaction%5B%5D=3&type%5B%5D=5&prix=0&action=search",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://miezimmo.fr/recherche/?transaction%5B%5D=3&type%5B%5D=6&prix=0&action=search",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h2[@class='article-title']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Miezimmo_PySpider_france")
        
        title = response.xpath("//h1/span/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        if "studio" in title.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))  
        
        external_id = response.xpath("substring-after(//h2/span/text()[contains(.,'Ref')],':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        rent = response.xpath("substring-after(//h2/span/text()[contains(.,'Prix')],':')").get()
        if rent:
            rent = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        if " – " in title:
            item_loader.add_value("address", title.split(" – ")[0].strip())
            item_loader.add_value("city", title.split(" – ")[0].strip())
        else:
            address = title.split("PIÈCE")[0].strip().split(" ")
            for i in address:
                if not i.isdigit():
                    item_loader.add_value("address", f"{i} ")
                    item_loader.add_value("city", f"{i} ")
            
        square_meters = response.xpath("//h3/span[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//h3/span[contains(.,'Chambre')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif "studio" in title.lower():
            item_loader.add_value("room_count", "1")
                
        description = " ".join(response.xpath("//div[@class='content-description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        import dateparser
        if "Disponible le" in description:
            available_date = description.split("Disponible le")[1].split("!")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[contains(@class,'galery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("substring-before(substring-after(//script[contains(.,'dpeges.dpe')]/text(),'value:'),',')").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        item_loader.add_value("landlord_name", "MIEZIMMO")
        item_loader.add_value("landlord_phone", "33 01 30 05 00 80")
        item_loader.add_value("landlord_email", "agence@miezimmo.fr")
        
        yield item_loader.load_item()