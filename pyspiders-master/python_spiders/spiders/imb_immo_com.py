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
    name = 'imbs_immo_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    external_source =  "Imb_immo_PySpider_united_kingdom" 
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.imbs-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1705156885363829&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.imbs-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1705156885363829&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C30&C_27_tmp=2&C_27_tmp=30&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):

        prop = response.meta.get('property_type')

        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='cell-product']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop})

            seen = True
        
        if page == 2 or seen:
            formdata={
                'aa_afunc': 'call',
                'aa_sfunc': 'get_products_search_ajax',
                'aa_cfunc': 'get_scroll_products_callback',
                'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":2,"nb_rows_per_page":6,"search_id":1705156885363829,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"1","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}'
            }
            url = f"https://www.imbs-immo.com/catalog/advanced_search_result.php"
            yield FormRequest(
                url, 
                formdata= formdata,
                callback=self.parse, meta={"page": page+1,"property_type":prop})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1[@class='product-title']/text()")

        item_loader.add_value("external_source", self.external_source)
        
        externalid= "".join(response.xpath("//div[@class='col-md-8 col-sm-8']/div[@class='product-model']/text()").extract())
        if externalid:
            item_loader.add_value("external_id", externalid.replace("Référence","").strip())
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        address = " ".join(response.xpath("//h1[@class='product-title']/span[@class='ville-title']/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(" ")[0].strip())
            item_loader.add_value("zipcode", address.split(" ")[-1].strip())

        rent = response.xpath("//div[@class='prix loyer']/span[contains(.,'Loyer')]/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.replace("\xa0","").split("Loyer")[1].split("€")[0].strip())

        item_loader.add_value("currency", "GBP")

        bathroom_count = " ".join(response.xpath("//div[contains(@class,'col-md-8')]/div/span[@class='value']/text()[contains(.,'Salle')]").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0].strip())

        room_count = " ".join(response.xpath("//div[contains(@class,'col-md-8')]/div/span[@class='value']/text()[contains(.,'chambre')]").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0].strip())
        else:
            room_count = " ".join(response.xpath("//div[contains(@class,'col-md-8')]/div/span[@class='value']/text()[contains(.,'pi')]").extract())
            if room_count:

                item_loader.add_value("room_count", room_count.strip().split("pi")[0].strip())

        deposit = " ".join(response.xpath("normalize-space(//div[@class='formatted_price_alur2_div']/span[@class='alur_location_depot']/text())").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.replace("\xa0","").split(":")[1].split("€")[0].strip())

        square_meters = " ".join(response.xpath("//div[contains(@class,'col-md-8')]/div/span[@class='value']/text()[contains(.,'m²')]").extract())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split("m²")[0].strip())

        description = " ".join(response.xpath("//div[@class='products-description']//text()").getall())
        if description:
            item_loader.add_value("description", re.sub("\s{2,}", " ", description))

        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product_short']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("energy_label","//div[@class='col-md-8 col-sm-8']/div[span[contains(.,'Classe')]]/div/text()")

        item_loader.add_value("landlord_name", "IMBS IMMOBILIERE")       
        item_loader.add_value("landlord_phone", "09.83.66.55.82")
         
     
        yield item_loader.load_item()