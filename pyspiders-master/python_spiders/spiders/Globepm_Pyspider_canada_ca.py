import requests
from scrapy import Spider, Request
import scrapy
from ..loaders import ListingLoader
pos = 1
prob = ''


class Royalyorkpropertymanagement_PySpider_ca_en(scrapy.Spider):
    name = 'globepm_ca'
    allowed_domains = ['globepm.ca']
    country = 'canada'
    locale = 'en'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):

        start_urls = ['https://www.globepm.ca/find-an-apartment#search']
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        urls = response.xpath('.//div[@class="view-property-link"]//a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield Request(url=url, callback=self.parse_area)

    def parse_area(self, response):
        global pos
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_source', self.external_source)
        title = response.xpath(".//strong[@class='page_heading has-globe-blue-color']//text()").extract()[0].strip()
        item_loader.add_value('title', title)
        phone = response.xpath('//html/body/div[1]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/text()[7]').extract()[0].strip()
        city = response.xpath('//html/body/div[1]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/text()[5]').extract()[0].strip()
        item_loader.add_value('city', city)
        address=response.xpath('//html/body/div[1]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/text()[3]').extract()[0].strip()+","+city
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()
        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal'] + responseGeocodeData['address']['PostalExt']
        city = responseGeocodeData['address']['City']
        # address = responseGeocodeData['address']['Match_addr']
        longitude = str(longitude)
        latitude = str(latitude)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)
        item_loader.add_value('longitude', longitude)
        item_loader.add_value('latitude', latitude)
        item_loader.add_value('address', address)
        item_loader.add_value('landlord_name', 'Globe Proberty Mangament')
        item_loader.add_value('landlord_phone', phone)
        item_loader.add_value('property_type', "apartment")
        item_loader.add_value('landlord_email', 'info@globepm.ca')
        untis =response.xpath('.//div[@id="available-unit"]//text()').extract()
        floor = response.xpath('.//div[@id="available-unit"]//a//@href').extract()
        imgs = response.xpath('.//img[@class="slideshow-img"]//@src').extract()
        desc =response.xpath('.//div[@id="about-section"]//text()').extract()[2].strip()
        amen=response.xpath('.//ul[@id="features-and-amenities-list"]//text()').extract()
        for i in amen :
            if "Balcony" in i :
                item_loader.add_value('balcony', True)
            if "Parking" in i :
                item_loader.add_value('parking', True)
            if "Dishwasher" in i :
                item_loader.add_value('dishwasher', True)
            if "Elevator" in i :
                item_loader.add_value('elevator', True)
        item_loader.add_value('description', desc)
        item_loader.add_value('images',imgs)
        item_loader.add_value("external_images_count",len(imgs))
        counter=0
        for i, j in enumerate(untis):
            if j == 'Starting From: ':
                if untis[i+1].strip() != "Call for Info" :
                    item_loader.add_value('floor_plan_images',floor[counter])
                    item_loader.add_value('available_date', untis[i+8].strip())
                    item_loader.replace_value("external_link", response.url + f"#{counter}")
                    item_loader.replace_value("rent", int(untis[i +1].strip().replace("$","").replace(".00","")))
                    item_loader.replace_value("currency", "CAD")
                    bed=untis[i -2].strip()[0]
                    if bed.isnumeric():
                        item_loader.replace_value("room_count", int(bed))
                    else:
                        item_loader.replace_value("room_count", 1)
                    item_loader.add_value("position",pos)
                    pos += 1
                    counter += 1
                    yield item_loader.load_item()




