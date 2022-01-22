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
import math

class MySpider(Spider):
    name = 'imbsimmo_com'
    execution_type='testing'
    country='france'
    locale='fr' # LEVEL 1
    scale_separator ='.'
    external_source = "Imbs_immo_PySpider_france"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.imbs-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1709135501727645&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                "property_type" : "apartment",
                "type": "1"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'),'house_type': url.get('type')})
    def parse(self, response):

        prop = response.meta.get('property_type')
        house_type = response.meta.get('house_type')
        last_page = response.meta.get('last_page',0)
        print("---->",last_page,prop)
        
        page = response.meta.get('page', 1)
        if page == 1:
            last_page = response.xpath("//div[@id='pages-infos']/@data-per-page").get()
        seen = False
        for item in response.xpath("//div[@class='cell-product']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            sale_type = item.xpath(".//div[@class='product-transac']//text()[.='Location']").get()
            if not sale_type:
                print(follow_url) 
                continue
            
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop})

            seen = True
        
        if last_page and page <int(last_page):
            page = page+1
            print(page)
            
            formdata = {
                'aa_afunc': 'call',
                'aa_sfunc': 'get_products_search_ajax',
                'aa_cfunc': 'get_scroll_products_callback',
                'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":'+str(page)+',"nb_rows_per_page":6,"search_id":1709135501727645,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":'+house_type+',"C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}'

            }
         
            url = f"https://www.imbs-immo.com/catalog/advanced_search_result.php"
            print(formdata)
            yield FormRequest(
                url, 
                formdata= formdata,
                callback=self.parse, meta={"page": page,"property_type":prop,"last_page":last_page,"house_type":house_type})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = "".join(response.xpath("//title/text()").extract())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        
        externalid=" ".join(response.xpath("//span[contains(.,'Ref.')]/text()").extract())
        if externalid:
            item_loader.add_value("external_id", externalid.split(":")[-1].strip())
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        address = " ".join(response.xpath("//h1[@class='product-title']/span[@class='ville-title']/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(" ")[0].strip())
            item_loader.add_value("zipcode", address.split(" ")[-1].strip())

        rent = response.xpath("//div[@class='prix loyer']/span[contains(.,'Loyer')]/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.replace("\xa0","").split("Loyer")[1].split("€")[0].split(".")[0].strip())

        item_loader.add_value("currency", "EUR")
        utilities=response.xpath("//span[@class='alur_location_charges']/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[1].split("€")[0].strip())

        bathroom_count = " ".join(response.xpath("//div[contains(@class,'col-md-8')]/div/span[@class='value']/text()[contains(.,'Salle')]").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0].strip())

        room_count1 = " ".join(response.xpath("//div[contains(@class,'col-md-8')]/div/span[@class='value']/text()[contains(.,'chambre')]").extract())
        room_count1=room_count1.strip().split(" ")[0].strip()
        room_count2 = " ".join(response.xpath("//div[contains(@class,'col-md-8')]/div/span[@class='value']/text()[contains(.,'pi')]").extract())
        room_count2=room_count2.strip().split("pi")[0].strip()
        if room_count1.isdigit() and room_count2.isdigit():
            item_loader.add_value("room_count", int(room_count2)+int(room_count1))

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


        item_loader.add_value("landlord_phone", "09 83 66 55 82")
        item_loader.add_value("landlord_name", " IMBS IMMOBILIERE")

        yield item_loader.load_item()
