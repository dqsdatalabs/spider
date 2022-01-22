import requests
import scrapy
from scrapy import Spider, Request
from ..loaders import ListingLoader


counter = 8
pos = 1
prob = ''


class Ziantoni_PySpider_italy_it(scrapy.Spider):
    name = 'lorandimmobiliare'
    allowed_domains = ['lorandimmobiliare.it']
    country = 'italy'
    locale = 'it'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):
        start_urls = ['https://www.lorandimmobiliare.it/immobili-in-affitto.html']
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        urls = response.xpath(f'.//a[@class="link-arrow tag price"]//@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield Request(url=url, callback=self.parse_area)
        next_page = response.xpath(f'//*[@id="properties"]/div[9]/ul/li//@href').extract()
        if len(next_page)>1:
            for i in range(1,len(next_page)):
                next_page = response.xpath(f'//*[@id="properties"]/div[9]/ul/li//@href').extract()[i]
                next_page = response.urljoin(next_page)
                yield Request(url=next_page, callback=self.parse)


    def parse_area(self, response):
        global pos
        desc = response.xpath('//*[@id="description"]/p[1]//text()').extract()[0].strip()
        comm = response.xpath('//*[@id="quick-summary"]/dl/dd[4]//text()').extract()[0]
        if 'Commerciale' in comm:
            yield
        else:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_source', self.external_source)
            title = response.xpath(f'//*[@id="property-detail"]/div[2]/div[1]/header/h2/text()').extract()[0]
            city = response.xpath('//*[@id="quick-summary"]/dl/dd[7]//text()').extract()[0].split(" ")[0]
            loc =response.xpath('//*[@id="quick-summary"]/dl/dd[8]//text()').extract()[0]
            prov=response.xpath('//*[@id="quick-summary"]/dl/dd[6]//text()').extract()[0].split(" ")[0]
            reg =response.xpath('//*[@id="quick-summary"]/dl/dd[5]//text()').extract()[0].split(" ")[0]
            address=f"{loc},{city},{prov},{reg}"
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()
            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal'] + responseGeocodeData['address']['PostalExt']
            # city = responseGeocodeData['address']['City']
            # address = responseGeocodeData['address']['Match_addr']
            longitude = str(longitude)
            latitude = str(latitude)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('longitude', longitude)
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('title', title)
            item_loader.add_value('city', city)
            item_loader.add_value("description",desc)
            rent = response.xpath('.//span[@class="tag price"]//text()').extract()[0].strip().replace( "€ ", "").replace(".", "")
            item_loader.add_value('rent', int(rent))
            item_loader.add_value('currency', "EUR")
            proptype = response.xpath('//*[@id="quick-summary"]/dl/dd[3]/a//text()').extract()[0].replace("-\r\n", "").replace(" ", "")
            if  "Attico" in proptype :
                item_loader.add_value('property_type', "house")
            elif "Appartamento" in proptype :
                item_loader.add_value('property_type', "apartment")
            elif "Studenti" in proptype :
                item_loader.add_value('property_type', "student_apartment")
            sq = response.xpath('//*[@id="quick-summary"]/dl/dd[9]//text()').extract()[0].split(" ")[0]
            item_loader.add_value("square_meters", int(sq))
            room = int(response.xpath('//*[@id="quick-summary"]/dl/dd[11]//text()').extract()[0].split(" ")[0])
            item_loader.add_value('room_count', room)
            item_loader.add_value("available_date",response.xpath('//*[@id="quick-summary"]/dl/dd[13]//text()').extract())
            bath = response.xpath('//*[@id="quick-summary"]/dl/dd[12]//text()').extract()[0].split(" ")[0]
            if bath.isnumeric() :
                item_loader.add_value('bathroom_count', int(bath))
            else :
                item_loader.add_value('bathroom_count', 1)

            utils = response.xpath('//li[@class="col-sm-4 col-xs-12"]//text()').extract()
            for i in utils :
                if "Balcone" in i :
                    item_loader.add_value('balcony', True)
                if "Ascensore" in i :
                    item_loader.add_value("elevator",True)
                if "Arredato" in i :
                    item_loader.add_value("furnished", True)
                if "Parcheggio" in i :
                    item_loader.add_value("parking", True)
                if "Lavanderia" in i :
                    item_loader.add_value("washing_machine", True)


            imgs = response.xpath('.//*[@id="property-gallery"]/div[1]/ul[1]//a//@href').extract()
            if imgs :
                pass
            else :
                imgs = response.xpath('//*[@id="property-foto"]//a//@href').extract()
            item_loader.add_value('images',imgs)
            pos += 1
            item_loader.add_value("address",address)
            item_loader.add_value("position",pos)
            id =response.xpath('//*[@id="property-detail"]/header/figure/text()[5]').extract()[0].strip()
            item_loader.add_value("external_id",id)
            item_loader.add_value("landlord_phone","+39 050 6200010")
            item_loader.add_value("landlord_name","Lorand Immobiliare")
            item_loader.add_value("landlord_email","info@lorandimmobiliare.it")
            yield item_loader.load_item()




