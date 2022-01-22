import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates


class TerracorpSpider(scrapy.Spider):
    name = 'terracorp'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['terracorp.ca']
    position = 1
    
    def start_requests(self):
        urls = [
            'https://terracorp.ca/properties_locations/properties_locations-27/',
            'https://terracorp.ca/properties_locations/properties_locations-28/',
            'https://terracorp.ca/properties_locations/properties_locations-25/',
            'https://terracorp.ca/properties_locations/properties_locations-26/',
            'https://terracorp.ca/properties_locations/properties_locations-24/',
            'https://terracorp.ca/properties_locations/properties_locations-23/',
            'https://terracorp.ca/properties_locations/properties_locations-22/',
            'https://terracorp.ca/properties_locations/properties_locations-21/',
            'https://terracorp.ca/properties_locations/properties_locations-2/',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for url in response.css(".property-link-button::attr(data-href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_page)


    def parse_page(self, response):
        title           = response.css(".property-name span::text").get().strip()
        city            = response.css(".property-name::text").get().strip()
        address         = response.xpath('//h2[contains(@class, "property-name")]/following-sibling::p/text()').get().strip()
        landlord_phone  = response.css(".property-telephone::text").get().strip()
        images          = [response.urljoin(i) for i in response.css(".orbit li img::attr(src)").getall()]
        description     = " ".join(response.css(".main-content p::text").getall())
        latitude, longitude = response.css("script").re("[0-9]+\.[0-9]+,\W*-[0-9]+\.[0-9]+")[0].split(",")
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        balcony,dishwasher,washing_machine, elevator, parking, swimming_pool = fetch_amenities(response.css(".large-3.columns ul li::text").getall())
        counter = 1
        for property in response.css(".unit-list-table tr"):
            if "not available." not in property.css(".available-date::text").get():
                rent            = property.css(".rent.unit-item::text").get().strip()
                if "$0" in rent:
                    continue
                else:
                    rent = int(rent.replace("$",""))

                property_type   = make_prop(property.css(".unit-type::text").get())
                room_count      = int(property.css(".bedroom.unit-item::text").get().strip())
                bathroom_count  = int(property.css(".bathroom.unit-item::text").get().strip())


                item = ListingLoader(response=response)
                item.add_value("external_link"          ,response.url+"#"+str(counter))
                item.add_value("external_source"        ,self.external_source)
                item.add_value("external_id"            ,response.url.split("=")[1])
                item.add_value("position"               ,self.position)
                item.add_value("title"                  ,title)
                item.add_value("address"                ,address)
                item.add_value("zipcode"                ,zipcode)
                item.add_value("city"                   ,city)
                item.add_value("latitude"               ,latitude)
                item.add_value("longitude"              ,longitude)
                item.add_value("property_type"          ,property_type)
                # item.add_value("square_meters"          ,square_meters)
                item.add_value("room_count"             ,room_count)
                item.add_value("bathroom_count"         ,bathroom_count)
                item.add_value("description"            ,description)
                item.add_value("currency"               ,"CAD")
                item.add_value("parking"                ,parking)
                item.add_value("images"                 ,images)
                item.add_value("balcony"                ,balcony)
                item.add_value("elevator"               ,elevator)
                item.add_value("rent"                   ,rent)
                item.add_value("dishwasher"             ,dishwasher)
                item.add_value("washing_machine"        ,washing_machine)
                item.add_value("swimming_pool"          ,swimming_pool)
                item.add_value("landlord_phone"         ,landlord_phone)
                # item.add_value("landlord_name"          ,landlord_name)
                # item.add_value("landlord_email"         ,landlord_email)
                self.position += 1
                counter += 1
                yield item.load_item()





def fetch_amenities(l):
    balcony,dishwasher,washing_machine, elevator, parking, swimming_pool = '','','','','',''
    for i in l:
        if i:
            i = i.lower()
            if 'balcon' in i:
                balcony = True
    
            elif 'dishwasher' in i:
                diswasher = True
            
            elif 'washer' in i or 'laundry' in i:
                washing_machine = True
            
            elif 'parking' in i or 'garage' in i:
                parking = True
    
            elif 'elevator' in i:
                elevator = True
            elif 'pool' in i:
                swimming_pool = True
            elif 'unfurnished' in i:
                furnished = False
            elif 'furnished' in i:
                furnished = True
    return balcony,dishwasher,washing_machine, elevator, parking, swimming_pool



def make_prop(prop):
    if not prop:
        return ""
    prop = prop.lower()

    if 'bachelor' in prop:
        return 'studio'
    elif 'town home' in prop:
        return "house"
    elif 'bedroom unit' in prop:
        return 'apartment'
    return prop