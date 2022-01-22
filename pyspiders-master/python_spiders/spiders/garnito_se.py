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
    name = 'garnito_se'
    execution_type='testing' 
    country='swenden'
    locale='sv'
    external_source = "Garnito_Pyspider_sweden"
    start_url = "https://webb.viteconline.se/Garnito/lagenheter"

    def start_requests(self):    

        yield Request(self.start_url, 
                callback=self.parse,
                )
    def parse(self, response):
        data=response.xpath("//script[contains(.,'GUID')]/text()").get()
        data=data.split("var objectList = ")[-1].split(";\r\n            var areaList")[0]
        for item in json.loads(data):
            url=f"https://webb.viteconline.se/Beskrivning.aspx?&typ=CMBoLgh&skin=Garnito&guid={item['GUID']}"
            yield Request(url, callback=self.populate_item,)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//h1/text()")
        property_type=response.xpath("//th[.='Objekttyp']/following-sibling::td/text()").get()
        if property_type and "Hyreslägenhet" in property_type:
            item_loader.add_value("property_type","apartment")
        adres="".join(response.xpath("//h1//text()").getall())
        if adres:
            item_loader.add_value("address",adres.split(",")[-2:])
        city=response.xpath("//h3[.='Kommun/Ort/Område']/following-sibling::p/text()").get()
        if city:
            item_loader.add_value("city",city.strip().replace("/"," ").split(" ")[1])
        images=response.xpath("//li[@class='royalSlide']//img//@src").getall()
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//th[.='Storlek']/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        rent=response.xpath("//th[.='Hyra']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("kr")[0].replace(" ",""))
        item_loader.add_value("currency","SEK")
        elevator=response.xpath("//th[.='Hiss']/following-sibling::td/text()").get()
        if elevator and "finns" in elevator:
            item_loader.add_value("elevator",True)
        description="".join(response.xpath("//h2[.='Lägenhetsbeskrivning']/parent::div/following-sibling::p/text() | //div[@id='ctl07_ctl00_0_ctl03_0_Texts2_0']//text()").getall())
        if description:
            description=description.replace("\r","").replace("\n","")
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description",description)
        room_count=response.xpath("//th[.='Antal rum']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("rum")[0])
        latitude=response.xpath("//script[contains(.,'paramLatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("L.marker")[-1].split(".addTo(map)")[0].split(",")[0].replace("([",""))
            item_loader.add_value("longitude",latitude.split("L.marker")[-1].split(".addTo(map)")[0].split(",")[1].replace("])","").strip())
        available_date=response.xpath("//th[.='Tillträde ']/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                    available_date, date_formats=["%d-%m-%Y"]
                )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
            item_loader.add_value("available_date",available_date)
        external_id=response.xpath("//th[.='Lägenhetsnummer']/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        item_loader.add_value("landlord_name","Annette Hansson Danfors Fastighetsförmedling")
        item_loader.add_value("landlord_email","annetten@danfors.se")
        item_loader.add_value("landlord_phone","0430 54 10 83")
        yield item_loader.load_item()