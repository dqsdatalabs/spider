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
    name = 'agenceliberty_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = {
        "PROXY_ON" : True
    }

    def start_requests(self):

        url =  "http://agenceliberty.fr/index.php?rubrique=liste&transac=louer"
        yield Request(url,
            callback=self.parse,dont_filter=True
            )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 0)
        seen = False
        for item in response.xpath("//div[@class='list-prog']/div//ul/li[@class='fullwidth']/a"):
            follow_url = response.urljoin(item.xpath(".//@href").extract_first())
            seen = True
            yield Request(follow_url, callback=self.populate_item)

        if page == 0 or seen:
            yield Request(f"http://agenceliberty.fr/index.php?rubrique=liste&dep={page}&type=all&prixmax=all&prixmin=all&piecesmin=all&piecesmax=all&ville=all&surfmin=all&surfmax=all&transac=louer&proxi=all", 
                            callback=self.parse, 
                            meta={"page": page + 6})

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        property_type = ""
        prop = response.xpath("//div[div[.='Type de bien']]/div[2]/text()").extract_first()
        if "APPARTEMENT" in prop:
            property_type ="apartment"
        elif "MAISON" in prop:
            property_type="house"
        else:
            return
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agenceliberty_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("external_id", "//div[div[.='Référence']]/div[2]/text()")

        item_loader.add_xpath("square_meters", "substring-before(//div[div[.='Surface habitable']]/div[2]/text(),'m²')")
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//div[@class='col-xs-6']/div/img/@src[contains(.,'DPE')],'DPE_'),'.')")
        

        rent = response.xpath("//div[@class='title col-xs-6'][.='Loyer']/following-sibling::div//text()").extract_first()
        if rent:
            price = rent.split("€")[0].replace(" ","").replace("\xa0","").strip()
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")

        utilities = "".join(response.xpath("//div[div[contains(.,'charges récupérables')]]/div[2]/text()").extract())
        if utilities:
            uti = utilities.strip().split("€")[0].replace(" ","").replace("\xa0","").strip()
            item_loader.add_value("utilities", uti.strip())

        room_count = "".join(response.xpath("//div[div[contains(.,'Pièce')]]/div[2]/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        deposit = "".join(response.xpath("//div[div[.='Dépôt de garantie']]/div[2]/text()").extract())
        if deposit:
            deposit = deposit.strip().replace("\xa0","").replace(" ","")
            item_loader.add_value("deposit", deposit.strip())
        
        address = "".join(response.xpath("//div[div[.='Localisation']]/div[2]/text()").extract())
        if address:
            item_loader.add_value("zipcode",address.split("(")[1].split(")")[0])
            item_loader.add_value("city",address.split("(")[0].strip())
            item_loader.add_value("address", "{} {}".format("".join(item_loader.get_collected_values("zipcode")),"".join(item_loader.get_collected_values("city"))))

        description = " ".join(response.xpath("//div[@class='contenu']/p//span//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())
        desc2=item_loader.get_output_value("description")
        if not desc2:
            desc1= " ".join(response.xpath("//div[@class='contenu']//p//strong//text()").getall())
            if desc1:
                item_loader.add_value("description", desc1.strip())


        available_date=response.xpath("//div[@class='contenu']/p//span//text()[contains(.,'Disponible')]").get()
        if available_date:
            date2 =  available_date.replace("Disponible","")
            if "DE SUITE" not in date2:
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                if date_parsed:
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)

        images = [x.split("(")[1].split(")")[0].strip() for x in response.xpath("//div[@class='swiper-slide']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            
        landlord_name = "".join(response.xpath("//h2[.='Contact']/following-sibling::p/text()[1]").extract())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_phone = response.xpath("substring-after(//h2[.='Contact']/following-sibling::p/text()[contains(.,'Tel')],': ')").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_value("landlord_phone", "(+33) 03 89 41 96 96")

        landlord_email = response.xpath("//h2[.='Contact']/following-sibling::p/a/text()").extract_first()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        else:
            item_loader.add_value("landlord_email", "contact@agenceliberty.fr")
            

        yield item_loader.load_item()
