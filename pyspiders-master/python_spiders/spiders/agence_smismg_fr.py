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
    name = 'agence_smismg_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = 'Agence_Smismg_PySpider_france'
    other_check = True
    def start_requests(self):

        ap_url = "https://agence-smismg.fr/recherche-avancee/page/{}/?filter_search_type[]=location&adv6_search_tab=location&term_id=233&term_counter=0&filter_search_action[]=1&nb-de-pieces=&advanced_city=&price_low_233=80&price_max_233=2000&submit=Rechercher"
        yield Request(
            url=ap_url.format(1),
            callback=self.parse,
            dont_filter=True,
            meta={
                "property_type":"apartment",
                "base":ap_url,
            }            
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'listing-unit')]/a[@target='_self']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(
                url=p_url,
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                    "base":base,
                }            
            )
        elif self.other_check:
            self.other_check = False
            ho_url = "https://agence-smismg.fr/recherche-avancee/page/{}/?filter_search_type%5B%5D=location&adv6_search_tab=location&term_id=233&term_counter=0&filter_search_action%5B%5D=2&nb-de-pieces=&advanced_city=&price_low_233=80&price_max_233=2000&submit=Rechercher"
            yield Request(
                url=ho_url.format(1),
                callback=self.parse,
                dont_filter=True,
                meta={
                    "property_type":"house",
                    "base":ho_url,
                }            
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("external_id", "//div/strong[contains(.,'ID')]/following-sibling::text()")

        address = response.xpath("//div[contains(@class,'intext_details')]/span/text()").get()
        if address:
            addr = address.lower().replace("(","").replace(")","").split(" ")
            for add in addr:
                if add.isdigit() and len(add) ==5:
                    item_loader.add_value("zipcode", add)
            
            if "rouen" in address.lower():
                item_loader.add_value("address", "Rouen")
                item_loader.add_value("city", "Rouen")
            else:
                if " à " in address:
                    address = address.split(" à ")[1]
                    item_loader.add_value("address", address)
                    item_loader.add_value("city", address.split("(")[0].strip())
                else:
                    address = address.split("m²")[1].strip()
                    item_loader.add_value("address", address)
                    city = address.split("(")[0].strip()
                    item_loader.add_value("city", city)

        rent = " ".join(response.xpath("//div/strong[contains(.,'Prix')]/following-sibling::text()").extract())
        if rent:
            price = rent.replace(" ","").replace("\xa0","").strip()
            item_loader.add_value("rent_string", price)

        deposit = " ".join(response.xpath("//div/strong[contains(.,'garantie')]/following-sibling::text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        external_id = " ".join(response.xpath("//div/strong[contains(.,'Référence')]/following-sibling::text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
            
        room_count = "".join(response.xpath("//div/strong[contains(.,'Chambre')]/following-sibling::text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div/strong[contains(.,'Pièces')]/following-sibling::text()")

        meters = " ".join(response.xpath("//div/strong[contains(.,'habitable')]/following-sibling::text()").extract())
        if meters:
            s_meters = meters.split("m")[0].strip()
            item_loader.add_value("square_meters", s_meters.strip())

        description = " ".join(response.xpath("//div[@class='panel-body']/p//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[@id='owl-demo']//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
            
        item_loader.add_xpath("latitude", "//div/@data-cur_lat")
        item_loader.add_xpath("longitude", "//div/@data-cur_long")
        utilities=item_loader.get_output_value("description")
        if utilities:
            utilities=utilities.split("CHARGES")[-1].split("euro")[0]
            utilities=re.findall("\d+",utilities)
            item_loader.add_value("utilities",utilities)
        
        item_loader.add_xpath("energy_label", "//div/strong[contains(.,'Classe')]/following-sibling::text()")

        bathroom_count = response.xpath("//div/strong[contains(.,'Salle')]/following-sibling::text()").get()
        if bathroom_count:
            if bathroom_count.strip() != "0":
                item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//div/strong[contains(.,'Parking')]/following-sibling::text()").get()
        if parking:
            if parking.strip() != "0":
                item_loader.add_value("parking",True)
            else:
                item_loader.add_value("parking",False)

        elevator = response.xpath("//div/strong[contains(.,'Ascenseur')]/following-sibling::text()").get()
        if elevator:
            if elevator.strip() != "0":
                item_loader.add_value("elevator",True)
            else:
                item_loader.add_value("elevator",False)
        
        terrace = response.xpath("//div/strong[contains(.,'Terrasse')]/following-sibling::text()").get()
        if terrace:
            if terrace.strip() != "0":
                item_loader.add_value("terrace",True)
            else:
                item_loader.add_value("terrace",False)
        
        balcony = response.xpath("//div/strong[contains(.,'Balcon')]/following-sibling::text()").get()
        if balcony:
            if balcony.strip() != "0":
                item_loader.add_value("balcony",True)
            else:
                item_loader.add_value("balcony",False)
        
        furnished = response.xpath("//div[@class='wpestate_estate_property_design_intext_details ']/span/text()").get()
        if furnished and "meuble" in furnished.lower():
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished",False)

        item_loader.add_xpath("floor", "//div/strong[contains(.,'Etage')]/following-sibling::text()")
        
        images = [x.split('url(')[1].split(')')[0] for x in response.xpath("//div[contains(@class,'image_gallery lightbox_trigger')]/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("landlord_phone", "02 35 70 84 84")
        item_loader.add_value("landlord_name", "Smismg")
        item_loader.add_value("landlord_email", "agence-smismg@wanadoo.fr")
        
        yield item_loader.load_item()