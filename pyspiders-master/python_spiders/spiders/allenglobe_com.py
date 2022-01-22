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
    name = 'allenglobe_com' # LEVEL 1
    execution_type='testing'
    country='spain'
    locale='es'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.allenglobe.com/es/buscador/alquiler/piso?&ascensor=&garaje=&terraza=&exterior=&pagina=1", 
            "property_type": "apartment"},
            {"url": "https://www.allenglobe.com/es/buscador/alquiler/chalet?&ascensor=&garaje=&terraza=&exterior=&pagina=1", 
            "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='large-12 left box_lista']"):
            url = item.xpath("./@onclick").extract_first()
            url_split = url.split("href='")[1].split("'")[0].strip()
            address = "".join(item.xpath(".//div[@class='large-6 left localizador columns']/text()").extract()).strip()
            follow_url = response.urljoin(url_split)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type"),"address":address})
            seen = True
        
        if page == 2 or seen:
            url = ""
            pt = ""
            if 'piso' in response.url:
                url = f"https://www.allenglobe.com/es/buscador/alquiler/piso?&ascensor=&garaje=&terraza=&exterior=&pagina={page}"
                pt = 'apartment'
            elif 'chalet' in response.url:
                url = f"https://www.allenglobe.com/es/buscador/alquiler/chalet?&ascensor=&garaje=&terraza=&exterior=&pagina={page}"
                pt = 'house'
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":pt})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Allenglobe_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        external_id=response.xpath("//div[@class='referencia']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[1].strip())
        
        title=response.xpath("//h4[@class='no-border']/text()").get()
        if title:
            item_loader.add_value("title", title)
            if "venta" in title.lower() and "alquiler" not in title.lower():
                return
        address =  response.meta.get('address')
        if address:
            address = str(address)
            item_loader.add_value("address",address)
            city = address 
            if "- " in address:
                city = address.split("- ")[-1]
            if "(" in city:
                city = city.split("(")[0].strip()
            item_loader.add_value("city", city)
        
        # address=response.xpath("//div[@class='row']/div/h6/text()").get()
        # if address:
        #     item_loader.add_value("address", address)

        rent=response.xpath("//p[@class='cont_precio']/span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
 
        
        room_count=response.xpath("//span[contains(.,'Hab')]/text()[not(contains(.,'Baño'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])

        
        square_meters=response.xpath("//span[@class='label'][contains(.,'const')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())

        bathroom = "".join(response.xpath("substring-before(//span[@class='label']/i[@class='fa-fw fa fa-bath']/following-sibling::text(),'Baños') ").extract())
        if bathroom:
            item_loader.add_xpath("bathroom_count", bathroom.strip())

        description = "".join(response.xpath("//p[@class='descripcion']/text()").getall()).strip().replace('\t', '').replace('\n', '').replace('\r', '')
        if description:
            item_loader.add_value("description", description)
        else:
            description = "".join(response.xpath("//p[@class='descripcion']/following-sibling::p//text()").getall()).strip().replace('\t', '').replace('\n', '').replace('\r', '')
            if description:
                item_loader.add_value("description", description)
        
        images=[response.urljoin(x) for x in response.xpath("//ul[@data-clearing='galeria_print']/li/a/img/@src").getall()]
        for image in images:
            item_loader.add_value("images", image)
            item_loader.add_value("external_images_count", str(len(images)))
        
        elevator=response.xpath("//span[@class='label'][contains(.,'Ascensor')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished= "".join(response.xpath("//span[@class='label'][contains(.,'Amueblado')]/text()").getall())
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace=response.xpath("//div[@class='ficha_tipo_1']/div/div//ul/li[contains(.,'Terraza')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name","Allenglobe Oviedo Centro")
        item_loader.add_value("landlord_phone","985204691")
        item_loader.add_value("landlord_email","oviedocentro@allenglobe.com")
        
        yield item_loader.load_item()