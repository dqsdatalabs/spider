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
    name = 'alain_rousseau_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.alain-rousseau.com/bien-particulier/recherche-particulier-location/?_sft_nc_cat_achat_louer=louer&_sft_nc_cat_type_bien=appartement&sf_paged={}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.alain-rousseau.com/bien-particulier/recherche-particulier-location/?_sft_nc_cat_achat_louer=louer&_sft_nc_cat_type_bien=maison&sf_paged={}",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='search-filter-results']/div[@class='listing_bien']/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Alain_Rousseau_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='titre'][contains(.,'Localisation')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='titre'][contains(.,'Localisation')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='titre'][contains(.,'Surface')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='titre'][contains(.,'Loyer')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='titre'][contains(.,'Référence')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='titre'][contains(.,'garantie')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@class='titre'][contains(.,'Charges')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"€":0, ".":0})
        
        if response.xpath("//div[@class='titre'][contains(.,'chambre')]/following-sibling::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='titre'][contains(.,'chambre')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//div[@class='titre'][contains(.,'pièces')]/following-sibling::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='titre'][contains(.,'pièces')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='titre'][contains(.,'salle')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[@class='titre'][contains(.,'étage')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slides']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='titre'][contains(.,'garage') or contains(.,'parking')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='titre'][contains(.,'balcon')]/following-sibling::div/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Alain Rousseau", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02 41 24 13 70", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="angers@alain-rousseau.com", input_type="VALUE")

        desc = " ".join(response.xpath("//div[@class='texte_descriptif_haut']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        available_date = "".join(response.xpath("//div[@class='titre'][contains(.,'Libre le')]/following-sibling::div/text()").getall())
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        energy_label = response.xpath("//div[contains(@class,'bloc_energie bloc')]//div[@class='texte']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        yield item_loader.load_item()