# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = "finomat_be"
    start_urls = ["http://www.finomat.be/loc.html"]  # LEVEL 1
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source='Finomat_PySpider_belgium'

    custom_settings = {
    "HTTPCACHE_ENABLED": False,
    }

    headers={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Host": "www.finomat.be",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Mobile Safari/537.36"
    }
    
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        for items in response.xpath("//td[@class='Vignette']"):
            
            prop_type = items.xpath(".//span[@class='NormalRouge']//b//text()").get()



            item = items.xpath(".//img[@class='image']/@src").get()
            if "Pictures" in item:
                detail_id = item.split("/Pictures/")[1].split("/")[0]
                follow_url = f"http://www.finomat.be/fiche/{detail_id}.html"
                yield Request(follow_url, callback=self.populate_item,meta={"prop_type":prop_type})
            

        if page < 12:
            url = f"http://www.finomat.be/loc/{page}.html"
            yield Request(url, callback=self.parse, meta={"page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//td/span[@class='NormalGrand']/b[2]/text()")
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("external_link", response.url)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("fiche/")[-1].split(".html")[0])
        item_loader.add_xpath("description", "//span[@class='Normal'][2]")
        address=response.xpath("//span[contains(.,'Région')]/text()").get()
        if address:
            item_loader.add_value("address",address.split(":")[-1])
        rent = response.xpath(
            "//span[@class='NormalGrand']//text()[contains(., '€')]"
        ).get()
        item_loader.add_value("rent", rent.split("€")[0].split(",")[0])

        item_loader.add_xpath("external_id", "//tr[th[.='Reference']]/td")
        meters = "".join(
            response.xpath(
                "//span[@class='Normal']//text()[contains(., 'Superficie habitable ')]"
            ).extract()
        )
        if meters:
            item_loader.add_value(
                "square_meters",
                meters.replace("Superficie habitable :", "").split("m²")[0],
            )


        property_type = response.meta.get("prop_type")
        if "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        elif "maison" in property_type.lower():
            item_loader.add_value("property_type","house")
        else:
            return

        item_loader.add_value("city","Namur")
        
        dontallow=" ".join(response.xpath("//span[@class='NormalGrand']//b//text()").getall())
        if dontallow and ("commerce" in dontallow or "industriel" in dontallow or "parking" in dontallow):
            return  
        room = response.xpath(
            "//span[@class='Normal']//text()[contains(.,'Chambres')]"
        ).get()
        if room:
            room = room.split("Chambres :")[0]
            if room != "":
                item_loader.add_xpath(
                    "room_count", "//tr[th[.='Number of bedrooms']]/td"
                )

        available_date = "".join(
            response.xpath(
                "//span[@class='Normal']//text()[contains(., 'Libre : ')]"
            ).extract()
        )
        if available_date: 
            item_loader.add_value(
                "available_date", available_date.replace("Libre :", "")
            )
        utilities = "".join(
            response.xpath(
                "//span[@class='Normal']//text()[(contains(., '-€'))]"
            ).extract()
        )
        if utilities:
            item_loader.add_value(
                "utilities", utilities.replace("Charges:", "").split(",")[0]
            )
        utilitiescheck=item_loader.get_output_value("utilities")
        if not utilitiescheck:
            utilities=item_loader.get_output_value("description")
            if "Charges" in utilities:
                item_loader.add_value("utilities",utilities.split("Charges :")[-1].split("€")[0])
        terrace = response.xpath(
            "//span[@class='NormalGrand']//text()[contains(., '/')]"
        ).get() 
        if terrace:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)
        id=item_loader.get_output_value("external_id")
        urlimage=f"http://www.finomat.be/include/ajax/agile_carousel_data.php?Id={id}"
        yield Request(urlimage, callback=self.image,headers=self.headers,meta={"item_loader":item_loader})
        item_loader.add_value("landlord_name","FINOMAT")
        item_loader.add_value("landlord_phone","081/22.10.12")
        item_loader.add_value("landlord_email","info@finomat.be")
        # yield item_loader.load_item()

    def image(self,response):
        print(response)
        # data=json.load(response.body)
        # sel = Selector(text=json.load(data)["html"], type="html") 
        # print(data)
        item_loader=response.meta.get("item_loader")
        images = [x for x in response.xpath("//a[@class='photo_link']//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        yield item_loader.load_item()


        
