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
from python_spiders.helper import ItemClear
class MySpider(Spider):
    name = 'agencerapp_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "http://agencerapp.fr/index.php?rubrique=Location&sous_rubrique=traitlouer"
    def start_requests(self):
        formdata = {
            "nbpieces": "all",
            "proxi": "0.15",
            "ville": "all",
            "type": "APPARTEMENT",
            "surfmin": "all",
            "surfmax": "all",
            "prixmin": "all",
            "prixmax": "all",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 8)
        seen = False
        for item in response.xpath("//div[@id='annonceplus']/@onclick").getall():
            slug = item.split('.href=')[1].strip().replace("'", "")
            follow_url = f"http://agencerapp.fr/{slug}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 8 or seen:
            p_url = f"http://agencerapp.fr/index.php?rubrique=Location&dep={page}&type=APPARTEMENT&nbpieces=all&ville=all&prixmin=all&prixmax=all&sous_rubrique=traitlouer&surfmin=all&surfmax=all&proxi=0.15"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+8})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(.,'RÉFÉRENCE')]//text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agencerapp_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//strong[contains(.,'Ville')]//parent::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//strong[contains(.,'Ville')]//parent::div/text()", input_type="F_XPATH", split_list={"(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//strong[contains(.,'Ville')]//parent::div/text()", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(.,'DESCRIPTIF DU BIEN :')]/../*[self::div][1]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//strong[contains(.,'Surface ')]//parent::div/text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        if response.xpath("//strong[contains(.,'Chambre')]//parent::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//strong[contains(.,'Chambre')]//parent::div/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        elif response.xpath("//strong[contains(.,'Pièce')]//parent::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//strong[contains(.,'Pièce')]//parent::div/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//td[contains(.,'Loyer')]//following-sibling::td//text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//td[contains(.,'Dépôt de garantie')]//following-sibling::td//text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={".":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[contains(@class,'group3')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img//@src[contains(.,'DPE')]", input_type="F_XPATH", split_list={"_":-1,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//strong[contains(.,'Etage')]//parent::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//td[contains(.,'charges')]//following-sibling::td//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="AGENCE IMMOBILIÈRE RAPP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 89 41 93 18", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="agence.rapp@calixo.net", input_type="VALUE")
      
        title = "".join(response.xpath("//span[contains(.,'en location')]/text() | //span[contains(.,'en location')]//span//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        yield item_loader.load_item()